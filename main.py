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

from peripheral import Peripheral, Actuator
from openmct import OpenMCTServer, TelemetryServer, buildOpenMCTjs
from midgard_functions import gv, print_out, get_element, combine_with_and, label, run, check_and_install_openmct, check_configs, check_peripherals, write_actuation, abort, unabort, shutdown


'''
prerequisite installed applications, git, nodejs(https://nodejs.org/en/download), npm

peripheral folder containing folders for each manufacturer, each manufacturer has files for each ID containing information about the sensors (raw data), processed data, actuators, and other variables. each system and model has a gerneric file defining system and model specific variables and default values and telling MIDGARD how to communicate with this specific device on different interfaces and what interfaces are valid (optional interfaces will be defined in the other variables setting). system folders can also contain custom sequences for that system and id files can contain whether they're valid

peripheral config files can inherit configs from other files in the structure parent = 'parent_file' for parent_file.toml in a manufacturer or parent = 'parent_folder/file_name' for parent_file.toml in parent_folder manufacturer

peripherals are made of elements (data streams, actuators, lockouts, interfaces, and phases)

for NI each cDAQ is a single peripheral

selectors can only be nominalized if the transition is valid. a selector can only be nominalized if the nominal state is in the current state's transition to. example: while a rocket is in state ascent, it cannot be nominalized to the state pad. 
selector nominal state should have all sources in its sources list because nominalization does not check for sources. 

openmct was built with npm run build and is being served by fastapi through python. sqlite is the telemetry server serving realtime and historical data. 

toml default is the state midgard writes automatically at startup, a value of false is the nominal/safe position (ie normally open or normally closed is the safe state and configured in hardware to equate with a value of false)

abort and logging actuators cannot have sources

if id and type are the same, the type folder will collapse and become the element object. if id and type are the same, no other elements of that type will be supported for that peripheral

cannot have duplicate ids on the same peripheral

currently, only switches can be tied to a physical actuator, but other control_types can call a function to actuate a switch for them. this can be expanded, but will require more control logic.

if a peripheral calls an actuation, midgard will ignore the elements in to_actuate when commanding the physical actuations (digital actuation will still occur) and rely on the peripheral to make those actuations without midgard needing to instruct them. the operating procedure is to have a duplicate of all actuators on both the peripheral and midgard with the propper configuration so propogated actuations occur the same on both systems (idealy use the same config file) (ie if ASGARD calls for main_lockout set to false, ASGARD and MIDGARD set everything armed by main_lockout False independently, but MIDGARD also sets the other peripherals that rely on what this peripheral actuated)

functions can be tied to any actuator, but buttons, switches, and selectors are recommended (at least for now). sequences just run code and gv.threads will run the function as a thread. buttons will run a sequence every actuation (except when they become inhibited) and gv.threads will be run forever (but cant have any armed or disarmed by and the thread must handle all safety logic) (BUTTON gv.threads ARE NOT RECOMENDED!). Switches will run a sequence or thread if the value is true. Selectors will run the function if the state has do_function and will run always if no states have do_function (will run the function when nominalization occurs if nominalized state is not inhibited and do_on_nominalization is True)

put in readme: Read the manual. life and limb could be at stake if you fail to understand how midgard actually works

'''


### Configuration Variables ###

# Data
log_data = True # Boolean. Log data to csv
log_actuations = True # Boolean. Log GUI commands to csv
simulated_data = True # Boolean. Discards peripheral data and replaces it with fake data. Meant for testing without actual peripherals/peripheral data.
keep_db = False # Boolean. If False, deletes the OpenMCT SQL database containing historical telemetry data. Recommended to keep True in a production environment in case of accidents and to reload data after a MIDGARD restart, but not strictly necessary.
keep_logs = False # Boolean. If False, deletes csv files after run. Should only be False in testing
log_output_dir = '' # String of absolute file path. Directory where to store log files. Will attempt to make directory if it doesn't already exist. If left blank (ie ''), will use logs folder inside current working directory.

# Display
print_actuations = True # Boolean. Print actuations to python terminal
show_server_logs = False # Boolean. Displays the terminal logs from the OpenMCT and telemetry servers

# Server
openmct_port = 4000 # Integer. The port to connect to OpenMCT
telemetry_port = 4001 # Integer. The port OpenMCT connects to for its historical data

# Safety
confirmation_keys = ['shift'] # List of keys for which keys to press to enable switch actuation. Uses AND logic. Leave empty for no confirmation [DANGEROUS!!!]. A list of valid keys (and how to write them for MIDGARD) can be found in valid_confirmation_keys.txt 
debug_mode = True # Boolean. Enables or disables debug mode, allowing certain actions that would not normally be permitted due to safety concerns (mostly for ground testing)
health_check_interval = 2 # Int or float. Interval at which the main thread checks for dead servers and gv.threads
openmct_dir = '' # String of absolute file path. Directory where OpenMCT is installed. Will attempt to install if it doesn't already exist. If left blank (ie ''), will use 'openmct' folder inside current working directory or build a new installation of OpenMCT (requires internet access).

# Peripherals
peripherals = {
    'Valhala_I':{'interface':'elrs', 'manufacturer':'VSS', 'id':'ASGARD_V0.2'},
    #'nidaq':{'interface':'ethernet', 'manufacturer':'NI', 'id':'1'},
    #'labjack':{'interface':'usb', 'manufacturer':'LabJack', 'id':'1'},
}


### End of Condifuration Variables. Do Not Edit Further ###


gv.peripherals = peripherals

gv.log_data = log_data
gv.log_actuations = log_actuations
gv.simulated_data = simulated_data
gv.keep_db = keep_db
gv.keep_logs = keep_logs
gv.log_output_dir = log_output_dir
gv.print_actuations = print_actuations
gv.show_server_logs = show_server_logs
gv.openmct_port = openmct_port
gv.telemetry_port = telemetry_port
gv.confirmation_keys = confirmation_keys
gv.debug_mode = debug_mode
gv.health_check_interval = health_check_interval
gv.openmct_dir = openmct_dir

if gv.log_output_dir == '':
    gv.log_output_dir = os.path.join(os.path.dirname(__file__), "logs")
if gv.openmct_dir == '':
    gv.openmct_dir = os.path.join(os.path.dirname(__file__), "openmct")

check_configs()

check_and_install_openmct() 

for name, args in gv.peripherals.items():
    gv.peripherals[name] = Peripheral(name, args) 
    
for p in gv.peripherals.values():
    for elem in p.actuators.values():
        group = elem.sync_group
        if group:
            gv.sync_groups.setdefault(group, []).append(elem)

buildOpenMCTjs()
gv.openmct = OpenMCTServer()
gv.telemetry = TelemetryServer()


def peripheral_worker(init_event):
    try:
        if gv.simulated_data:
            i = 0
            j = False
            str_list = ['a','b','c','d','e','f','g','h','i','j','k']
            
        init_event.set()
        print_out(f"[PeripheralHandler] started")
        try:
            while not gv.stop_event.is_set():
                data = {}
                if gv.simulated_data:
                    time.sleep(0.1)
                    sample = {}
                    for key in gv.all_keys:
                        element = get_element(gv.data_tree, key)
                        if element.element_class == 'Data_Stream':
                            if element.unit == 'bool': sample[key] = j
                            elif element.unit == 'str': sample[key] = str_list[math.floor(i/10)]
                            else: sample[key] = i
                    #sample['Valhala_I.Actuator.main_lock'] = j
                    i = 0 if i == 100 else i + 1
                    j = not j if i == 100 else j
                    ts = int(time.time() * 1000)
                    data = {ts:sample}
                else:
                    for peripheral in gv.peripherals.values():
                        if peripheral.manufacturer == 'NI':
                            try:
                                data = peripheral.data_queue.get(timeout=0.2)
                            except Empty:
                                continue
                            except Exception as e:
                                _, _, tb = sys.exc_info()
                                print_out(f"[PeripheralHandler] NI Read Error: {type(e).__name__} on line {tb.tb_lineno}: {e}") 
                            
                        elif peripheral.manufacturer == 'LabJack':
                            try:
                                ts_end = int(time.time() * 1000)
                                ret = ljm.eStreamRead(peripheral.handle) # get data from labjack
                                aData = ret[0]
                                
                                if not aData:
                                    continue
                                
                                deinterleaved = np.array([aData[index::peripheral.numAddresses] for index in range(peripheral.numAddresses)]) # labjack returns a 1D arary, convert to 2d
                                
                                sensor_data = []
                                for channel in peripheral.channels:
                                    chanNumAbs = peripheral.channels.index(channel)
                                    channelData = deinterleaved[chanNumAbs]
                                    sensor_data.append(channelData)
                                sensor_data = np.array(sensor_data).T
                                
                                for i, sample in enumerate(sensor_data): # Number of data samples
                                    ts = ts_end - (len(sensor_data) - i) * (1/peripheral.pull_freq) * 1e9  # the timestamp we get is from the last data point so we need to calculate the timestamps backwards from this
                                    data[ts] = {}
                                    for j, channel in enumerate(peripheral.channels): # Number of channels
                                        data[ts][channel.key] = sample[j] * channel.scale + channel.offset
                            except Exception as e:
                                _, _, tb = sys.exc_info()
                                print_out(f"[PeripheralHandler] LabJack Read Error: {type(e).__name__} on line {tb.tb_lineno}: {e}") 
                                    
                        elif peripheral.interface.type == 'radio':
                            if peripheral.interface.protocol == 'mavlink':
                                pass
                            elif peripheral.interface.protocol == 'elrs':
                                pass
                
                #print_out(data)
                max_utc = max(data)
                for ts, sample in sorted(data.items()):
                    for stream_key, value in sample.items():
                        element = get_element(gv.data_tree, stream_key)
                        if ts == max_utc:
                            gv.telemetry.send(ts, stream_key, value, element, "peripheral")
                            if element.element_class in ['Actuator', 'State']:
                                write_actuation(key, value, "peripheral", do_print=gv.print_actuations)
                        else:
                            gv.telemetry.send(ts, stream_key, value, element, "peripheral", push_to_gui=False)
                
        except Exception as e:
            _, _, tb = sys.exc_info()
            print_out(f"[PeripheralHandler] Read Error: {type(e).__name__} on line {tb.tb_lineno}: {e}") 
                
    except Exception as e:
        if not gv.stop_event.is_set():
            _, _, tb = sys.exc_info()
            print_out(f"[PeripheralHandler] Setup Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
    finally: 
        print_out(f"[PeripheralHandler] stopped")

def gui_worker(init_event):
    # Handles all user inputs from GUI
    try:
        for key in gv.all_keys: # Populate default values
            element = get_element(gv.data_tree, key)
            default = getattr(element, 'default', None)
            if default is not None:
                write_actuation(key, default, "auto", do_print=False)

        init_event.set()
        print_out(f"[GUIHandler] started")
        while not gv.stop_event.is_set():
            try:
                cmd = gv.telemetry.command_queue.get(timeout=0.1)
            except Empty:
                continue
            
            pressed_keys = [str(k).lower() for k in cmd.get("pressed_keys", [])]
                
            key, requested = cmd["key"], cmd["requested"]

            element = get_element(gv.data_tree, key)
            if element is None:
                print_out(f"[GUIHandler] Unknown GUI Key Error: '{key}'")
                continue
            
            missing_keys = [k for k in gv.confirmation_keys if k not in pressed_keys]
            write_actuation(key, requested, "user", missing_keys=gv.missing_keys, do_print=gv.print_actuations)
                    
                
    except Exception as e:
        if not gv.stop_event.is_set():
            _, _, tb = sys.exc_info()
            print_out(f"[GUIHandler] Initial Write Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
    finally: # write nominal states for all switches
        for key in gv.all_keys: # Populate default values
            element = get_element(gv.data_tree, key)
            default = getattr(element, 'nominal', None)
            if default is not None:
                write_actuation(key, default, "auto", do_print=False)
        print_out(f"[GUIHandler] stopped")


def main():
    try:
        # Check peripherals
        check_peripherals()
        
        # Create servers
        gv.openmct.start()
        gv.servers.append(("OpenMCTServer", gv.openmct))
        gv.telemetry.start()
        gv.servers.append(("TelemetryServer", gv.telemetry))
        
        # Create gv.threads
        peripheral_init_event = threading.Event()
        peripheral_handler = threading.Thread(target=peripheral_worker, name="PeripheralHandler", args=(peripheral_init_event,))
        peripheral_handler.daemon = True
        peripheral_handler.start()
        gv.threads.append(("PeripheralHandler", peripheral_handler))
        
        gui_init_event = threading.Event()
        gui_handler = threading.Thread(target=gui_worker, name="GUIHandler", args=(gui_init_event,))
        gui_handler.daemon = True
        gui_handler.start()
        gv.threads.append(("GUIHandler", gui_handler))
        
        while not (peripheral_init_event.is_set() and gui_init_event.is_set()): # wait for all gv.threads to initialize 
            time.sleep(.1)
        
        write_actuation(gv.peripherals['Valhala_I'].elements['flight_phase'].key, 'pad', "auto", do_print=False) ##### TEMP!!! #####
        
        gv.startup_event.set()
        print_out("\n\nPress Ctrl+C to stop...\n\n")
        while not gv.stop_event.is_set():
            try:
                start = time.perf_counter()
                
                # Check for dead servers
                dead_servers = [(name, s) for name, s in gv.servers if not s.is_running]
                if dead_servers:
                    print_out(f"[HealthCheck] Warning: Dead servers detected: {[name for name, _ in dead_servers]}")
                    
                # Check for dead gv.threads
                dead_threads = [(name, t) for name, t in gv.threads if not t.is_alive()]
                if dead_threads:
                    print_out(f"[HealthCheck] Warning: Dead gv.threads detected: {[name for name, _ in dead_threads]}")
                
                # Wait for next interval
                while (time.perf_counter() - start < gv.health_check_interval) and not gv.stop_event.is_set():
                    if gv.non_abort_shutdown.is_set(): # check for special conditions
                        if not gv.stop_event.is_set():
                            shutdown(do_abort=False)
                    time.sleep(.05)
                
            except Exception as e:
                _, _, tb = sys.exc_info()
                print_out(f"[HealthCheck] Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")# write initial display with settings
                time.sleep(1)
        if not gv.stop_event.is_set():
            shutdown()
        
    except KeyboardInterrupt as e:
        if not gv.stop_event.is_set():
            shutdown()

    except Exception as e:
        _, _, tb = sys.exc_info()
        print_out(f"[Main Thread] Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
        if not gv.stop_event.is_set():
            shutdown()

if __name__ == '__main__':
    main()

'''
# mavlink

COMPLETED:
    Works offline
    Checks config variables
        Warnings for certain config variables like debug_mode and keep_logs


TO DO
    Bugs

    Configuration
        Check configurations are valid
            No duplicate ids of the same element class
            Check that peripheral ids are valid
            Check no duplicates in id arrays in toml files in _resolve_inheritance
            Check that no peripherals in python are using the same external device 
            Check that all the members of sync group have the same values (except name, id, description, etc)
            Check sequences use a supported control_type
            Check that actuators have no buttons in armed_by and disarmed_by
            Check that states only have other states in their selector in transition_to
        Armed and disarmed from another peripheral
    Overseer
        Restart of MIDGARD or peripheral handling
        Peripheral communication
    GUI
        Add cluster control_type
        Restart frontend while python is running?
        Plugins
            Servo Alignment
            Map (Geofence and Course)
            Fuel Bar
    Data Handling
        Recieving data from peripherals
        Import csv logs into database
    Actuator Control
        Actuating radio peripherals
        Add servo and cluster support to functions
    Writing
        Readme
        How to
        Flight procedure
    Testing
        NI
        LabJack
    Other Peripherals
        MAVLink / Ardupilot / Q Ground Control / Mission Planner / PX4 / MavProxy
        KSP (KSP OpenMCT and Telemachus Reborn https://gitlab.com/overloader-ksp/kerbal-telemetry)
        Ansys STK
        Basilisk (BSK)

'''