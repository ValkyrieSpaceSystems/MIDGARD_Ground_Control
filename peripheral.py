import time, csv, os, threading, sys, shutil, re, tomllib, math, pynput, csv, tomllib, importlib.util, asyncio, json, sqlite3, uvicorn, subprocess, urllib.request, urllib.error, socket
import numpy as np
from queue import PriorityQueue, Queue, Empty, Full
from datetime import datetime, UTC

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

import nidaqmx
from nidaqmx import stream_readers, DaqReadError
from nidaqmx.stream_writers import DigitalSingleChannelWriter, DigitalMultiChannelWriter
from nidaqmx.constants import TerminalConfiguration, AcquisitionType, LineGrouping, WAIT_INFINITELY
import nidaqmx.system
from labjack import ljm
import Basilisk

from midgard_functions import gv, print_out, get_element, combine_with_and, label, run, check_and_install_openmct, check_configs, check_peripherals, write_actuation, abort, unabort, shutdown


PERIPHERALS_ROOT = os.path.join(os.path.dirname(__file__), "Peripherals")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _load_toml(manufacturer: str, id_name: str) -> str:
    path = os.path.join(PERIPHERALS_ROOT, manufacturer, f"{id_name}.toml")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config not found: {path}")
    with open(path, "rb") as f:
        return tomllib.load(f)


def _deep_merge(base: dict, override: dict) -> dict:
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        elif key in result and isinstance(result[key], list) and isinstance(value, list):
            result[key] = _merge_lists_by_id(result[key], value)
        else:
            result[key] = value
    return result


def _merge_lists_by_id(base: list, override: list) -> list:
    base_map: dict[str, dict] = {
        item["id"]: item.copy() for item in base if "id" in item
    }
    no_id = [item for item in base if "id" not in item]
    for item in override:
        if "id" in item:
            if item["id"] in base_map:
                base_map[item["id"]].update(item)
            else:
                base_map[item["id"]] = item
        else:
            no_id.append(item)
    return list(base_map.values()) + no_id


def _resolve_inheritance(manufacturer: str, id_name: str, _seen: set | None = None) -> dict:
    """
    Recursively resolve `parent` chain and deep-merge configs.
    Parent files are looked up in the same manufacturer folder.
    """
    _seen = _seen or set()
    key = (manufacturer, id_name)
    if key in _seen:
        raise ValueError(f"Circular inheritance: {manufacturer}/{id_name}")
    _seen.add(key)

    raw = _load_toml(manufacturer, id_name)
    parent = raw.get("meta", {}).get("parent", None)
    
    if not parent:
        return raw
    else:
        parent = parent.split('/')
        if len(parent) == 1:
            parent_manufacturer = manufacturer
            parent_id = parent[0]
        elif len(parent) == 2:
            parent_manufacturer = parent[0]
            parent_id = parent[1]
        else:
            raise [f'[MIDGARD WARNING] peripheral {id_name} has unsupported parent structure {parent}. Must be "parent_file" or "parent_manufacturer/parent_file"']

    parent_raw = _resolve_inheritance(parent_manufacturer, parent_id, _seen)
    merged = _deep_merge(parent_raw, raw)
    merged.get("meta", {}).pop("parent", None)
    return merged


def _load_element_function(manufacturer: str, module_name: str, function_name: str):
    """
    Dynamically loads a function from a .py file living alongside this
    manufacturer's TOML configs, e.g. Peripherals/VSS/ignition_sequence.py.
    Each call produces an independent module object (never cached in
    sys.modules), so identical filenames across manufacturers never collide.
    """
    path = os.path.join(PERIPHERALS_ROOT, manufacturer, f"{module_name}.py")
    if not os.path.exists(path):
        print_out(f"[MIDGARD WARNING] Module not found: {path}")
        return None

    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        print_out(f"[MIDGARD WARNING] Failed to load module '{module_name}': {e}")
        return None

    func = getattr(module, function_name, None)
    if func is None:
        print_out(f"[MIDGARD WARNING] Function '{function_name}' not found in '{module_name}'")
        return None
    return func


# ── Element Classes ──────────────────────────────────────────────────────────


class Interface:
    def __init__(self, entry: dict):
        self.element_class: str   = "Interface"
        self.id: str              = entry["id"]
        self.name: str            = entry.get("name", label(self.id))
        self.description: str     = entry.get("description", "")
        self.type: str            = entry["type"]
        self.protocol: str | None = entry.get("protocol")
        self.baud: int | None     = entry.get("baud")
        # radio fields
        self.freq: int | None     = entry.get("freq")
        self.unit: str | None     = "None"
        self.remove: bool         = entry.get("remove", False)

    def __repr__(self):
        return f"Interface({self.id!r}, type={self.type!r})"


class State:
    def __init__(self, entry: dict):
        self.element_class: str     = "State"
        self.id: str                = entry["id"]
        self.name: str              = entry.get("name", label(self.id))
        self.description: str       = entry.get("description", "")
        self.type: str              = entry.get("type", "")
        self.parent: str            = entry.get("parent", "")
        self.transition_to: list    = entry.get("transition_to", [])
        self.transition_from        = []
        self.armed_by: list[str]    = entry.get("armed_by", [])
        self.disarmed_by: list[str] = entry.get("disarmed_by", [])
        self.arms                   = []
        self.disarms                = []
        self.sources: list[str]     = entry.get("sources", [])
        self.do_function: bool      = entry.get("do_function", False)
        self.control_type           = 'state'
        self.index: int | None      = None
        self.value: bool            = False
        self.nominal: bool          = False
        self.remove: bool           = entry.get("remove", False)

    def __repr__(self):
        return f"State({self.id!r}, type={self.type!r}, value={self.value})"


class Actuator:
    def __init__(self, entry: dict, parent):
        self.element_class: str        = "Actuator"
        self.parent                    = parent
        self.id: str                   = entry["id"]
        self.name: str                 = entry.get("name", label(self.id))
        self.description: str          = entry.get("description", "")
        self.type: str                 = entry["type"]
        self.subtype: str | None       = entry.get("subtype", None)
        self.normally: str | None      = entry.get("normally", None)
        self.function_file             = entry.get("function_file", None) # file
        self.function_name             = entry.get("function", None) # function in file, default function is run()
        self.function_type             = entry.get("function_type", None) # trigger, sequence, thread
        self.module_num: int | None    = entry.get("module", None) 
        self.module                    = None
        self.writer                    = None
        self.channel: int | str | None = entry.get("channel", None) # 1, FIN0
        self.sync_group: str | None    = entry.get("sync_group")
        self.armed_by: list[str]       = entry.get("armed_by", [])
        self.disarmed_by: list[str]    = entry.get("disarmed_by", [])
        self.arms                      = []
        self.disarms                   = []
        self.sources: list[str]        = entry.get("sources", [])
        self.debug_only: bool          = entry.get("debug_only", False)
        self.debug_sources: list[str]  = entry.get("debug_sources", [])
        self.switch_display: dict      = entry.get("switch_display", {})
        self.remove: bool              = entry.get("remove", False)
        
        if parent.debug_mode:
            self.sources = list(dict.fromkeys(list(self.sources) + self.debug_sources))
        if self.function_file and self.function_name:
            self.func = _load_element_function(parent.manufacturer, self.function_file, self.function_name)
            if self.function_type == 'thread':
                self.thread = None
                self.stop_event = threading.Event()
                     
        
                
        if self.type == 'button':
            self.control_type  = 'button'
                        
        elif self.type == 'switch': 
            self.control_type      = 'switch'
            self.nominal_state     = 'Off'
            self.off_nominal_state = 'On'
            
        elif self.type == 'selector':
            self.control_type       = 'selector'
            self.states: dict       = {}
            self.states_index: list = []
            self.states_name: list  = []
            
        elif self.type == 'message': 
            self.control_type = 'message'
                    
        elif self.type == 'abort': 
            self.control_type      = 'switch'
            self.nominal_state     = 'Safe'
            self.off_nominal_state = 'Aborted'
            
        elif self.type == 'lockout': 
            self.control_type      = 'switch'
            self.nominal_state     = 'Disarmed'
            self.off_nominal_state = 'Armed'
            
        elif self.type == 'thread':
            self.control_type      = 'switch'
            self.nominal_state     = 'Stopped'
            self.off_nominal_state = 'Running'
            
        elif self.type == 'valve': 
            self.control_type  = 'switch'
            if self.normally == 'Open':
                self.nominal_state = 'Open'
                self.off_nominal_state = 'Closed'
            else:
                self.nominal_state = 'Closed'
                self.off_nominal_state = 'Open'
            
        elif self.type == 'servo': 
            self.control_type                = None
            self.pin: int | None             = entry.get("pin")
            self.unit: str | None            = entry.get("unit")
            self.range: tuple | None         = tuple(entry["range"]) if "range" in entry else None
            self.slew_rate_max: float | None = entry.get("slew_rate_max")
            self.scale: float | None         = entry.get("scale")
            self.offset: float | None        = entry.get("offset")
            
        elif self.type == 'pyro': 
            self.control_type          = 'switch' # cluster
            self.cluster_quantity      = 2
            self.pyro_channel: int | None   = entry.get("channel", None)
            self.pyro_channel_a: int | None = entry.get("channel_a", None)
            self.pyro_channel_b: int | None = entry.get("channel_b", None)
            self.nominal_state         = 'Unfired'
            self.off_nominal_state     = 'Fired'
        
        elif self.type == 'ssr':
            self.control_type = 'switch'
            self.nominal_state     = 'Off'
            self.off_nominal_state = 'On'
        
        else:
            print_out(f'[MIDGARD WARNING] Element {self.name} has unknown type {self.type}')
            
                       
        if self.control_type == "switch":
            self.default: bool = entry.get("default", False)
            self.nominal: bool = False
        elif self.control_type == "button":
            self.default = 1
            self.nominal = 1
        elif self.control_type == "selector":
            self.default: str | int | None = entry.get("default", None)
            self.nominal: str | int | None = entry.get("nominal", self.default)
            self.run_on_nominalize: bool   = entry.get("run_on_nominalize", False)
        elif self.control_type == "message":
            self.default = ''
            self.nominal = ''
        elif self.control_type == "cluster":
            self.default: list[bool] = [entry.get("default", False)] * self.cluster_quantity
            self.nominal: list[bool] = [entry.get("nominal", entry.get("default", False))] * self.cluster_quantity
        elif self.control_type == None:
            self.default: float | None = entry.get("default", None)
            self.nominal: float | None = entry.get("nominal", self.default)
            
        if self.type == 'pyro': # Hard coded for safety
            self.default: bool = False
            self.nominal: bool = False
            
        
        
        # ── Build selector-type actuators from sibling [[state]] entries ──────────
        if self.control_type == 'selector':
            self.states = {s.id: s for s in self.parent.states.values() if s.parent == self.id}
            self.states_index = list(self.states.values())
            self.states_name = [s.name for s in self.states_index]
            self.function_states = [s for s in self.states_index if s.do_function]
            
            for index, state in enumerate(self.states_index):
                state.index = index
                state.parent = self
                
            for state in self.states_index:
                resolved = []
                for ref in state.transition_to:
                    target = self.parent.states.get(ref.split('.')[-1])
                    if target is None:
                        print_out(f"[MIDGARD WARNING] State '{state.id}' transitions to unknown state '{ref}'")
                        continue
                    target = self.states.get(ref.split('.')[-1])
                    if target is None:
                        print_out(f"[MIDGARD WARNING] State '{state.id}' transitions to state not in its selector'{ref}'")
                        continue
                    resolved.append(ref)
                state.transition_to = resolved
                    
            if self.default in self.states.keys():
                self.default = self.states[self.default].index
            else:
                self.default = 0
                print_out(f"[MIDGARD WARNING] Selector '{self.id}' default '{self.default}' not found in selector states {self.states}")
                    
            if self.nominal in self.states.keys():
                self.nominal = self.states[self.nominal].index
            else:
                self.nominal = 0
                print_out(f"[MIDGARD WARNING] Selector '{self.id}' nominal '{self.nominal}' not found in selector states {self.states}")   
        
        self.value = self.default
        self.key = None
        
        
    def __repr__(self):
        if hasattr(self, 'unit'):
            return f"Actuator({self.id!r}, type={self.type!r}, value={self.value} {self.unit})"
        else:
            return f"Actuator({self.id!r}, type={self.type!r}, value={self.value})"


class DataStream:
    def __init__(self, entry: dict, parent):
        self.element_class: str           = "Data_Stream"
        self.parent                       = parent
        self.id: str                      = entry["id"]
        self.name: str                    = entry.get("name", label(self.id))
        self.description: str             = entry.get("description", "")
        self.type: str                    = entry["type"]
        self.subtype: str | None          = entry.get("subtype", None)
        self.priority: int                = entry.get("priority", 2) #0-4
        self.module_num: int | None       = entry.get("module", None) 
        self.module                       = None
        self.channel: int | str | None    = entry.get("channel", None) # 1, AIN0
        self.unit: str | None             = entry.get("unit", None)
        self.range: tuple                 = tuple(entry.get("range", []))
        self.nominal: tuple               = tuple(entry.get("nominal", []))
        self.component: str | None        = entry.get("component")
        self.sample_rate_hz: float | None = entry.get("sample_rate_hz", None)
        self.scale: float | int | None    = entry.get("scale",1)
        self.offset: float | int | None   = entry.get("offset",0)
        # runtime state
        self.control_type                 = None
        self.value: float | None          = None
        self.key                          = None
        self.remove: bool                 = entry.get("remove", False)

    def in_range(self) -> bool:
        if self.value is None:
            return True
        return self.range[0] <= self.value <= self.range[1]

    def in_nominal(self) -> bool:
        if self.value is None:
            return True
        return self.nominal[0] <= self.value <= self.nominal[1]

    def __repr__(self):
        if self.unit == None:
            return f"DataStream({self.id!r}, type={self.type!r}, value={self.value})"
        else:
            return f"DataStream({self.id!r}, type={self.type!r}, value={self.value} {self.unit})"


class NIModule:
    def __init__(self, entry: dict, parent):
        self.element_class: str     = "NI_Module"
        self.parent                 = parent
        self.id: str                = entry["id"]
        self.name: str              = entry.get("name", label(self.id))
        self.description: str       = entry.get("description", "")
        self.type: int              = entry["type"] #9205, 9253, 9213, etc
        self.module_num: int | None = entry.get("module", None) # 0, 1, 2, etc
        self.pull_freq: int | None  = entry.get("pull_freq", None)
        self.push_freq: int | None  = entry.get("push_freq", None)
        self.remove: bool           = entry.get("remove", False)
        
        self.channels = []
        self.task = None
        self.reader = None
        self.writer = None
        
        for device in self.parent.devices:
            device_cdaq = device.name[device.name.find('cDAQ'):device.name.find('Mod')]
            device_mod_num = int(device.name[device.name.find('Mod')+3:])-1
            if device_cdaq == self.parent.device and device_mod_num == self.module_num:
                if self.type == 9205: # Voltage (Pressure Transducer)
                    #print_out(f"NI-9205 Analog Voltage Input Module {device.name} Connected")
                    try:
                        self.task = nidaqmx.Task()
                        self.task.ai_channels.add_ai_voltage_chan(
                            f'{device.name}/ai0:31',
                            terminal_config=TerminalConfiguration.RSE,
                            min_val=-10, max_val=10
                        )
                        self.channels = [None] * 32
                        self.parent.tasks[self.id] = self.task
                    except Exception as e:
                        _, _, tb = sys.exc_info()
                        raise ConnectionError(f"NI Initialization Error: Failed to add PT voltage channels {device.name}: {type(e).__name__} on line {tb.tb_lineno}: {e}")
    
                elif self.type == 9253: # Current (Pressure Transducer)
                    #print_out(f"NI-9253 Analog Current Input Module {device.name} Connected")
                    try:
                        self.task = nidaqmx.Task()
                        self.task.ai_channels.add_ai_current_chan(
                            f'{device.name}/ai0:7',
                            terminal_config=TerminalConfiguration.RSE,
                            min_val=-0.02, max_val=0.02, # ±20 mA range
                            units=nidaqmx.constants.CurrentUnits.AMPS
                        )
                        self.channels = [None] * 8
                        self.parent.tasks[self.id] = self.task
                    except Exception as e:
                        _, _, tb = sys.exc_info()
                        raise ConnectionError(f"NI Initialization Error: Failed to add PT current channels {device.name}: {type(e).__name__} on line {tb.tb_lineno}: {e}")
    
                elif self.type == 9213: # Thermocouple
                    #print_out(f"NI-9213 Thermocouple Module {device.name} Connected")
                    try:
                        self.task = nidaqmx.Task()
                        self.task.ai_channels.add_ai_thrmcpl_chan(
                        physical_channel=f'{device.name}/ai0:7',
                        min_val=-200, max_val=1260,
                        thermocouple_type=nidaqmx.constants.ThermocoupleType.K,
                        cjc_source=nidaqmx.constants.CJCSource.BUILT_IN
                        )
                        self.channels = [None] * 8
                        self.parent.tasks[self.id] = self.task
                    except Exception as e:
                        _, _, tb = sys.exc_info()
                        raise ConnectionError(f"NI Initialization Error: Failed to add channels {device.name}: {type(e).__name__} on line {tb.tb_lineno}: {e}")
    
                elif self.type == 9237: # Load Cell
                    #print_out(f"NI-9237 Load Cell Module {device.name} Connected")
                    try:
                        self.task = nidaqmx.Task()
                        self.task.ai_channels.add_ai_force_bridge_table_chan(
                            f"{device.name}/ai0:3",
                            min_val=0,
                            max_val=2000,
                            voltage_excit_val=10,
                            nominal_bridge_resistance=700,
                            electrical_vals=[0, -0.3710, -0.7418, -1.0200, -1.3909, -1.8547],
                            physical_vals=[0, 400, 800, 1100, 1500, 2000]
                        )
                        self.channels = [None] * 4
                        self.parent.tasks[self.id] = self.task
                    except Exception as e:
                        _, _, tb = sys.exc_info()
                        raise ConnectionError(f"NI Initialization Error: Failed to add LC channels {device.name}: {type(e).__name__} on line {tb.tb_lineno}: {e}")
                    
                elif self.type == 9485: # SSR
                    #print_out(f"NI-9485 SSR Module {device.name} Connected")
                    self.task = nidaqmx.Task()
                    self.writer = DigitalMultiChannelWriter(self.task.out_stream)
                    for line in range(8):
                        self.task.do_channels.add_do_chan(f"{device.name}/port0/line{line}")
                    self.writer.write_one_sample_one_line(np.array([False]*8)) # Write all channels nominal at start
                    self.channels = [None] * 8
                    self.parent.writers[self.id] = self.writer
                    
                else:
                    #print_out(f"Unknown NI Module {device.product_type} {device.name}. Ignoring")
                    # check if any data_streams or modules use this 
                    raise
        
        for element in list(self.parent.actuators.values()) + list(self.parent.data_streams.values()):
            if element.module_num == self.module_num:
                element.module = self
                self.channels[element.channel] = element
        
        if self.type in [9205, 9253, 9213, 9237]:
            self.update_interval = round(self.pull_freq / self.push_freq)
            self.task.timing.cfg_samp_clk_timing(rate=self.pull_freq, sample_mode=AcquisitionType.CONTINUOUS)
            self.task.in_stream.input_buf_size = self.pull_freq * 5 
            self.task.in_stream.overwrite = nidaqmx.constants.OverwriteMode.OVERWRITE_UNREAD_SAMPLES
            self.reader = stream_readers.AnalogMultiChannelReader(self.task.in_stream)
            
            self.task.register_every_n_samples_acquired_into_buffer_event(self.update_interval, self.callback)
            self.task.start()
            
        elif self.type in [9485]:
            self.task.start()
        
    def callback(self, task_idx, event_type, num_samples, cb_data=None):
        '''generic_callback(num_samples, 'PT', pt_units, pt_reader, num_pt_chan, pt_scaling, pt_names)'''
        try:
            ts_end = int(time.time() * 1000)
            data = {}
            buffer = np.zeros((len(self.channels), num_samples), dtype=np.float64)
            self.reader.read_many_sample(buffer, num_samples, timeout=WAIT_INFINITELY)
            sensor_data = buffer.T # convert from arrays for each sensor full of data points for each timestamp to arrays for each timestamp full of data points for each sensor
            if len(sensor_data) == 0:
                return 0

            sensor_data = np.array(sensor_data).T
            
            max_priority = 0
            for i, sample in enumerate(sensor_data): # Number of data samples
                ts = ts_end - (len(sensor_data) - i) * (1/self.pull_freq) * 1e9  # the timestamp we get is from the last data point so we need to calculate the timestamps backwards from this
                data[ts] = {}
                for j, channel in enumerate(self.channels): # Number of channels
                    data[ts][channel.key] = sample[j] * channel.scale + channel.offset
                    max_priority = max(max_priority, channel.priority)
                    
            self.parent.data_queue.put((max_priority, data))
        except Exception as e:
            _, _, tb = sys.exc_info() 
            print_out(f"[PeripheralHandler] Error: {self.parent.display_name} module {self.name} ({self.module_num}) Callback Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")


# ── Peripheral class ───────────────────────────────────────────────────────────

class Peripheral:
    def __init__(self, name: str, args: dict):
        self.stop_event = gv.stop_event
        self.debug_mode = gv.debug_mode
        self.name:         str = name
        self.interface_id: str = args["interface"]
        self.manufacturer: str = args["manufacturer"]
        self.id:           str = args["id"]
        self.data_queue        = PriorityQueue(maxsize=10000)

        raw  = _resolve_inheritance(self.manufacturer, self.id)
        meta = raw.get("meta", {})
        
        # ── Meta ───────────────────────────────────────────────────────
        self.display_name: str     = meta.get("name", label(self.id))
        self.description: str      = meta.get("description", "")
        self.type: str             = meta.get("type", "unknown")
        self.notes: str            = meta.get("notes", "")
        self.phase: str            = meta.get("first_phase", "")
        self.switch_display: dict  = meta.get("switch_display", {})
        self.device: str           = meta.get("device", []) #cDAQ1, T7, etc
        self.pull_freq: int | None = meta.get("pull_freq", None)
        self.push_freq: int | None = meta.get("push_freq", None)
        
        # ── Config Manufacturer ────────────────────────────────────────────
        if self.manufacturer == 'NI':
            self.system = nidaqmx.system.System.local()
            self.devices = self.system.devices
            self.tasks = {}
            self.writers = {}
            
        elif self.manufacturer == 'LabJack':
            self.handle = ljm.openS(self.device, "ANY", "ANY")

        # ── Element Dicts ────────────────────────────────────────────
        self.interfaces: dict[str, Interface] = {e["id"]: Interface(e) for e in raw.get("interface", [])}
        self.states: dict[str, State] = {e["id"]: State(e) for e in raw.get("state", [])}
        self.actuators: dict[str, Actuator] = {e["id"]: Actuator(e, self) for e in raw.get("actuator", [])}
        self.data_streams: dict[str, DataStream] = {e["id"]: DataStream(e, self) for e in raw.get("data_stream", [])}
        self.ni_modules: dict[str, NIModule] = {e["id"]: NIModule(e, self) for e in raw.get("ni_modules", [])}
        
        self.elements = self.interfaces | self.states | self.actuators | self.data_streams | self.ni_modules
        self.element_lookup = {
            'Interface': self.interfaces,
            'State': self.states,
            'Actuator': self.actuators,
            'Data_Stream': self.data_streams,
            'NI_Module': self.ni_modules
        }

        # ── Configure LabJack ──────────────────────────────────────────\
        if self.manufacturer == 'LabJack':
            self.labjack_actuators = [element for element in self.actuators.values() if element.channel is not None]
            
            # LabJack Config
            ljm.eWriteName(self.handle, "STREAM_TRIGGER_INDEX", 0) # Ensure triggered stream is disabled.
            ljm.eWriteName(self.handle, "STREAM_CLOCK_SOURCE", 0) # Enabling internally-clocked stream.
            
            # AIN ranges are +/-10 V and stream resolution index is 0 (default).
            aNames = ["AIN_ALL_RANGE", "STREAM_RESOLUTION_INDEX"]
            aValues = [10.0, 0]

            # set to single ended and auto settling time
            aNames.extend(["AIN_ALL_NEGATIVE_CH", "STREAM_SETTLING_US"])
            aValues.extend([ljm.constants.GND, 0])
            
            self.channels = [elem for elem in self.data_streams if elem.channel is not None]
            self.aScanListNames = [elem.channels for elem in self.channels]
            self.numAddresses = len(self.aScanListNames)
            aScanList = ljm.namesToAddresses(self.numAddresses, self.aScanListNames)[0]
            
            sensor_update_interval = round(self.pull_freq / self.sensor_push_freq)
            
            ljm.eStreamStart(self.handle, sensor_update_interval, self.numAddresses, aScanList, self.pull_freq)

        # ── Update Armed and Disarmed With Element Objects ─────────────
        for element in self.elements.values():
            if hasattr(element, 'armed_by'):
                for cond_key in element.armed_by:
                    cond_index = element.armed_by.index(cond_key)
                    cond_id = cond_key.split('.')[-1]
                    cond = self.elements[cond_id]
                    element.armed_by[cond_index] = cond
                    cond.arms.append(element)
            
            if hasattr(element, 'disarmed_by'):
                for cond_key in element.disarmed_by:
                    cond_index = element.disarmed_by.index(cond_key)
                    cond_id = cond_key.split('.')[-1]
                    cond = self.elements[cond_id]
                    element.disarmed_by[cond_index] = cond
                    cond.disarms.append(element)
            
            if hasattr(element, 'transition_to'):
                for cond_key in element.transition_to:
                    cond_index = element.transition_to.index(cond_key)
                    cond_id = cond_key.split('.')[-1]
                    cond = self.elements[cond_id]
                    element.transition_to[cond_index] = cond
                    cond.transition_from.append(element)
                element.transition_to.append(element)
                element.transition_from.append(element)

        # ── Remove elements ────────────────────────────────────────────
        to_remove = []
        for element in self.elements.values():
            if element.remove == True:
                reasons = []
                if hasattr(element, 'arms'):
                    reasons.append(f"it arms {[elem.name for elem in element.arms]}")
                if hasattr(element, 'transition_to'):
                    for elem in element.transition_to:
                        if elem.transition_from == [element]:
                            reasons.append(f"it is the only way to transition to {elem.name}")

                if reasons:
                    print_out(f"[MIDGARD WARNING] element {element.name} cannot be removed because {combine_with_and(reasons)}")
                else:
                    to_remove.append(element)
                    
        for element in to_remove:
            self.element_lookup[element.element_class].pop(element.id)
            self.elements.pop(element.id)
            
        # ── Select active interface ────────────────────────────────────
        self.interface: Interface | None = self.interfaces.get(self.interface_id)
        if self.interface is None:
            print_out(f"[MIDGARD WARNING] Interface '{self.interface_id}' not found in {self.id}")

        # ── Validate Config ────────────────────────────────────────────
        #put validation code here


    def __repr__(self):
        return f"Peripheral({self.name!r}, id={self.id!r})"