"""
peripheral.py — MIDGARD peripheral config loader

File structure:
    Peripherals/
        <manufacturer>/
            <id>.toml           ← e.g. VSS/ASGARD_V0.2.toml
            rocket.toml         ← shared parent configs live here too

Usage:
    peripherals = {
        'rocket': {'interface': 'elrs', 'manufacturer': 'VSS', 'id': 'ASGARD_V0.2'},
    }
    for name, args in peripherals.items():
        peripherals[name] = peripheral(name, args)
"""

import os, tomllib, csv
from typing import Any
from utility import get_element, combine_with_and


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
    parent_id = raw.get("meta", {}).get("parent")

    if not parent_id:
        return raw

    parent_raw = _resolve_inheritance(manufacturer, parent_id, _seen)
    merged = _deep_merge(parent_raw, raw)
    merged.get("meta", {}).pop("parent", None)
    return merged


# ── element classes ──────────────────────────────────────────────────────────


class Interface:
    def __init__(self, entry: dict):
        self.element_type: str    = "Interface"
        self.id: str              = entry["id"]
        self.name: str            = entry.get("name", self.id)
        self.description: str     = entry.get("description", "")
        self.type: str            = entry["type"]
        self.protocol: str | None = entry.get("protocol")
        self.baud: int | None     = entry.get("baud")
        # radio fields
        self.freq: int | None     = entry.get("freq")
        self.unit: str | None     = "None"

    def __repr__(self):
        return f"Interface({self.id!r}, type={self.type!r})"


class State:
    def __init__(self, entry: dict):
        self.element_type: str      = "State"
        self.id: str                = entry["id"]
        self.name: str              = entry.get("name", self.id)
        self.description: str       = entry.get("description", "")
        self.type: str              = entry.get("type", "")
        self.parent: str            = entry.get("parent", "")
        self.transition_to: list    = entry.get("transition_to", [])
        self.armed_by: list[str]    = entry.get("armed_by", [])
        self.disarmed_by: list[str] = entry.get("disarmed_by", [])
        self.arms                   = []
        self.disarms                = []
        self.sources: list[str]     = entry.get("sources", []) 
        self.index: int | None      = None
        self.value: bool            = False
        self.nominal: bool          = False

    def __repr__(self):
        return f"State({self.id!r}, type={self.type!r}, value={self.value})"


class Actuator:
    def __init__(self, entry: dict, parent):
        self.element_type: str      = "Actuator"
        self.parent                 = parent
        self.id: str                = entry["id"]
        self.name: str              = entry.get("name", self.id)
        self.description: str       = entry.get("description", "")
        self.type: str              = entry["type"]
        self.subtype: str | None    = entry.get("subtype")
        self.armed_by: list[str]    = entry.get("armed_by", [])
        self.disarmed_by: list[str] = entry.get("disarmed_by", [])
        self.arms                   = []
        self.disarms                = []
        self.sources: list[str]     = entry.get("sources", []) 
        self.debug_only: bool       = entry.get("debug_only", False)
        self.switch_display: dict   = entry.get("switch_display", {})
        
        
        if self.type == 'selector':
            self.control_type        = 'selector'
            self.default: str | None = entry.get("default", None)
            self.nominal: bool       = entry.get("nominal", self.default)
            self.states: dict        = {}
            self.states_index: list  = []
            self.states_name: list   = []
                
        elif self.type == 'lockout': 
            self.control_type      = 'switch'
            self.default: bool     = entry.get("default", False)
            self.nominal: bool     = entry.get("nominal", self.default)
            self.nominal_state     = 'Disarmed'
            self.off_nominal_state = 'Armed'
                
        elif self.type == 'logging': 
            self.control_type      = 'switch'
            self.default: bool     = entry.get("default", False)
            self.nominal: bool     = entry.get("nominal", self.default)
            self.nominal_state     = 'Off'
            self.off_nominal_state = 'On'
            
        elif self.type == 'trigger':
            self.control_type        = 'button'
            self.default             = 1
            self.nominal             = 1
            self.trigger: str | None = entry.get("trigger", None)
            
        elif self.type == 'sequence':
            self.control_type         = 'switch'
            self.default              = None
            self.nominal              = None
            self.sequence: str | None = entry.get("sequence", None)
            
        elif self.type == 'valve': 
            self.control_type  = 'switch'
            self.default: bool = entry.get("default", False)
            self.nominal: bool = entry.get("nominal", self.default)
            if self.default == False:
                self.nominal_state = 'Closed'
                self.off_nominal_state = 'Open'
            elif self.default == True:
                self.nominal_state = 'Open'
                self.off_nominal_state = 'Closed'
            
        elif self.type == 'servo': 
            self.control_type                = None
            self.pin: int | None             = entry.get("pin")
            self.unit: str | None            = entry.get("unit")
            self.range: tuple | None         = tuple(entry["range"]) if "range" in entry else None
            self.default: float | None       = entry.get("default_position")
            self.nominal: float | None       = entry.get("nominal_position") or self.default
            self.slew_rate_max: float | None = entry.get("slew_rate_max")
            self.scale: float | None         = entry.get("scale")
            self.offset: float | None        = entry.get("offset")
            
        elif self.type == 'pyro': 
            self.control_type          = 'switch' # cluster
            self.cluster_quantity      = 2
            self.channel: int | None   = entry.get("channel")
            self.channel_a: int | None = entry.get("channel_a")
            self.channel_b: int | None = entry.get("channel_b")
            self.default: bool         = False
            self.nominal: bool         = False
            self.nominal_state         = 'Unfired'
            self.off_nominal_state     = 'Fired'
        
        else:
            self.control_type = 'switch'
        
        
        # ── Build selector-type actuators from sibling [[state]] entries ──────────
        if self.control_type == 'selector':
            self.states = {s.id: s for s in self.parent.states.values()}
            self.states_index = list(self.parent.states.values())
            self.states_name = [s.name for s in self.states_index]
            
            for index, state in enumerate(self.states_index):
                state.index = index
                state.parent = self
                
            for state in self.states_index:
                resolved = []
                for ref in state.transition_to:
                    target = self.parent.states.get(ref.split('.')[-1])
                    if target is None:
                        print(f"[MIDGARD WARNING] State '{state.id}' transitions to unknown state '{ref}'")
                        continue
                    target = self.states.get(ref.split('.')[-1])
                    if target is None:
                        print(f"[MIDGARD WARNING] State '{state.id}' transitions to state not in its selector'{ref}'")
                        continue
                    resolved.append(ref)
                state.transition_to = resolved
                    
            if self.default in self.states.keys():
                self.default = self.states[self.default].index
            else:
                self.default = 0
                print(f"[MIDGARD WARNING] Selector '{self.id}' default '{self.default}' not found in selector states {self.states}")
                    
            if self.nominal in self.states.keys():
                self.nominal = self.states[self.nominal].index
            else:
                self.nominal = 0
                print(f"[MIDGARD WARNING] Selector '{self.id}' nominal '{self.nominal}' not found in selector states {self.states}")   
        
        self.value = self.default
        self.key = None
        
                
    def __repr__(self):
        if not hasattr(self, 'unit'):
            return f"Actuator({self.id!r}, type={self.type!r}, value={self.value})"
        else:
            return f"Actuator({self.id!r}, type={self.type!r}, value={self.value} {self.unit})"


class DataStream:
    def __init__(self, entry: dict, parent):
        self.element_type: str            = "Data_Stream"
        self.parent                       = parent
        self.id: str                      = entry["id"]
        self.name: str                    = entry.get("name", self.id)
        self.description: str             = entry.get("description", "")
        self.type: str                    = entry["type"]
        self.subtype: str | None          = entry.get("subtype", None)
        self.unit: str | None             = entry.get("unit", None)
        self.range: tuple                 = tuple(entry.get("range", []))
        self.nominal: tuple               = tuple(entry.get("nominal", []))
        self.component: str | None        = entry.get("component")
        self.sample_rate_hz: float | None = entry.get("sample_rate_hz")
        self.scale: float | None          = entry.get("scale")
        self.offset: float | None         = entry.get("offset")
        # runtime state
        self.control_type                 = None
        self.value: float | None          = None
        self.key                          = None

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
            return f"Actuator({self.id!r}, type={self.type!r}, value={self.value})"
        else:
            return f"Actuator({self.id!r}, type={self.type!r}, value={self.value} {self.unit})"


# ── Peripheral class ───────────────────────────────────────────────────────────

class Peripheral:
    def __init__(self, stop_event, name: str, args: dict):
        self.name:         str = name
        self.interface_id: str = args["interface"]
        self.manufacturer: str = args["manufacturer"]
        self.id:           str = args["id"]

        raw  = _resolve_inheritance(self.manufacturer, self.id)
        meta = raw.get("meta", {})

        # ── Meta ───────────────────────────────────────────────────────
        self.display_name: str    = meta.get("name", self.id)
        self.description: str     = meta.get("description", "")
        self.type: str            = meta.get("type", "unknown")
        self.notes: str           = meta.get("notes", "")
        self.phase: str           = meta.get("first_phase", "")
        self.switch_display: dict = meta.get("switch_display", {})

        # ── Element Dicts ────────────────────────────────────────────
        self.interfaces: dict[str, Interface] = {e["id"]: Interface(e) for e in raw.get("interface", [])}
        self.states: dict[str, State] = {e["id"]: State(e) for e in raw.get("state", [])}
        self.actuators: dict[str, Actuator] = {e["id"]: Actuator(e, self) for e in raw.get("actuator", [])}
        self.data_streams: dict[str, DataStream] = {e["id"]: DataStream(e, self) for e in raw.get("data_stream", [])}
        
        self.elements = self.interfaces | self.states | self.actuators | self.data_streams

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
                
        

        # ── Select active interface ────────────────────────────────────
        self.active_interface: Interface | None = self.interfaces.get(self.interface_id)
        if self.active_interface is None:
            print(f"[MIDGARD WARNING] Interface '{self.interface_id}' not found in {self.id}")

        # ── Validate references ────────────────────────────────────────
        self._validate()

    # ── Validation ─────────────────────────────────────────────────────

    def _validate(self):
        pass
        # check that first phase is a valid phase


    def __repr__(self):
        return f"Peripheral({self.name!r}, id={self.id!r})"