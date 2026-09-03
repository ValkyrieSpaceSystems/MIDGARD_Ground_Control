import time, csv, os, threading, sys, shutil, re, tomllib, math, pynput, csv, tomllib, importlib.util, asyncio, json, sqlite3, uvicorn
import numpy as np
from queue import Queue, Empty, Full
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

from peripheral import Peripheral
from openmct import OpenMCTServer, TelemetryServer, buildOpenMCTjs
from midgard_functions import get_element, combine_with_and, label, check_configs, write_actuation, abort, unabort, shutdown


'''
peripheral folder containing folders for each manufacturer, each manufacturer has files for each ID containing information about the sensors (raw data), processed data, actuators, and other variables. each system and model has a gerneric file defining system and model specific variables and default values and telling MIDGARD how to communicate with this specific device on different interfaces and what interfaces are valid (optional interfaces will be defined in the other variables setting). system folders can also contain custom sequences for that system and id files can contain whether they're valid

peripherals are made of elements (data streams, actuators, lockouts, interfaces, and phases)

for NI each cDAQ is a single peripheral


openmct was built with npm run build and is being served by fastapi through python. sqlite is the telemetry server serving realtime and historical data. 

toml default is the state midgard writes automatically at startup, a value of false is the nominal/safe position (ie normally open or normally closed is the safe state and configured in hardware to equate with a value of false)

abort and logging actuators cannot have sources

if id and type are the same, the type folder will collapse and become the element object. if id and type are the same, no other elements of that type will be supported for that peripheral

cannot have duplicate ids of the same element class

if a peripheral calls an actuation, midgard will ignore the elements in to_actuate when commanding the physical actuations (digital actuation will still occur) and rely on the peripheral to make those actuations without midgard needing to instruct them. the operating procedure is to have a duplicate of all actuators on both the peripheral and midgard with the propper configuration so propogated actuations occur the same on both systems (idealy use the same config file) (ie if ASGARD calls for main_lockout set to false, ASGARD and MIDGARD set everything armed by main_lockout False independently, but MIDGARD also sets the other peripherals that rely on what this peripheral actuated)

sequences can be tied to any actuator (ehhhh), but buttons, switches, and selectors are recommended (at least for now). regular sequences just run code and threaded sequences will run the sequence as a thread. regular sequences are only supported by buttons (will run every actuation), switches (will run when True), and selectors (will run every change). threaded sequences are only supported by switches (will run when True) and selectors (will run always). 

put in readme: Read the manual. life and limb could be at stake if you fail to understand how midgard actually works

'''


# Configuration Variables

# Data
log_data = True # Boolean. Log data to csv
log_actuations = True # Boolean. Log GUI commands to csv
simulated_data = True # Boolean. Discards peripheral data and replaces it with fake data. Meant for testing without actual peripherals/peripheral data.
keep_csv = False # Boolean. If False, deletes csv files after run. Should only be False in testing
log_output_dir = '' # String of absolute file path. Directory where to store log files. Will attempt to make directory if it doesn't already exist. If left blank (ie ''), will use logs folder inside current working directory.

# Display
print_switch_changes = True # Boolean. Print switch state changes to python terminal
show_server_logs = False # Boolean. Displays the terminal logs from the OpenMCT and telemetry servers

# Safety
confirmation_keys = [pynput.keyboard.Key.shift] # List of single character strings for keys or pynput.keyboard.Key for which keys to press to enable switch actuation. Uses AND logic. Leave empty for no confirmation [DANGEROUS!!!]
debug_mode = True # Boolean. Enables or disables debug mode, allowing certain actions that would not normally be permitted due to safety concerns (mostly for ground testing)
health_check_interval = 2 # Int or float. Interval at which the main thread checks for dead servers and threads
openmct_dir = '' # String of absolute file path. Directory where OpenMCT is installed. Will attempt to install if it doesn't already exist. If left blank (ie ''), will use 'openmct' folder inside current working directory or build a new installation of OpenMCT (requires internet access).


peripherals = {
    'Valhala_I':{'interface':'elrs', 'manufacturer':'VSS', 'id':'ASGARD_V0.2'},
    #'nidaq':{'interface':'ethernet', 'manufacturer':'NI', 'id':'1'},
    #'labjack':{'interface':'usb', 'manufacturer':'LabJack', 'id':'1'},
}


threads = []
servers = []
stop_event = threading.Event()
abort_state = threading.Event()
non_abort_shutdown = threading.Event()
startup_event = threading.Event()

for name, args in peripherals.items():
    peripherals[name] = Peripheral(stop_event, debug_mode, name, args)
    
sync_groups: dict[str, list] = {}
for p in peripherals.values():
    for elem in p.actuators.values():
        group = elem.sync_group
        if group:
            sync_groups.setdefault(group, []).append(elem)

data_tree, all_keys = buildOpenMCTjs(peripherals)

#print(data_tree)
#print(all_keys)

openmct = OpenMCTServer(port=4000, openmct_dir=openmct_dir, show_logs=show_server_logs)
telemetry = TelemetryServer(stop_event, log_data=log_data, log_actuations=log_actuations, log_output_dir=log_output_dir, port=4001, show_logs=show_server_logs)

global_vars = (debug_mode, show_server_logs, simulated_data, log_data, log_actuations, print_switch_changes, keep_csv, openmct_dir, log_output_dir, confirmation_keys, peripherals, threads, servers, stop_event, abort_state, non_abort_shutdown, startup_event, sync_groups, data_tree, all_keys, openmct, telemetry)


def peripheral_worker(init_event):
    try:
        if simulated_data:
            i = 0
            j = False
            str_list = ['a','b','c','d','e','f','g','h','i','j','k']
            
        init_event.set()
        print(f"[PeripheralHandler] started")
        try:
            while not stop_event.is_set():
                data = {}
                if simulated_data:
                    time.sleep(0.1)
                    sample = {}
                    for key in all_keys:
                        element = get_element(data_tree, key)
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
                    for peripheral in peripherals.values():
                        if peripheral.manufacturer == 'NI':
                            try:
                                data = peripheral.data_queue.get(timeout=0.2)
                            except Empty:
                                continue
                            
                        elif peripheral.manufacturer == 'LabJack':
                            ts_end = int(time.time() * 1000)
                            ret = ljm.eStreamRead(peripheral.handle) # get data from labjack
                            aData = ret[0]
                            
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
                                    
                        elif peripheral.interface.type == 'radio':
                            if peripheral.interface.protocol == 'mavlink':
                                pass
                            elif peripheral.interface.protocol == 'elrs':
                                pass
                
                #print(data)
                max_utc = max(data)
                for ts, sample in sorted(data.items()):
                    for stream_key, value in sample.items():
                        if ts == max_utc:
                            telemetry.send(ts, stream_key, value, data_tree, "peripheral")
                            if print_switch_changes and stream_key.split('.')[1] in ['Actuator', 'State']:
                                write_actuation(global_vars, key, value, "peripheral", do_print=print_switch_changes)
                        else:
                            telemetry.send(ts, stream_key, value, data_tree, "peripheral", push_to_gui=False)
                
        except Exception as e:
            _, _, tb = sys.exc_info()
            print(f"[PeripheralHandler] Read Error: {type(e).__name__} on line {tb.tb_lineno}: {e}") 
                
    except Exception as e:
        if not stop_event.is_set():
            _, _, tb = sys.exc_info()
            print(f"[PeripheralHandler] Setup Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
    finally: 
        print(f"[PeripheralHandler] stopped")
        
def gui_worker(init_event):
    # Handles all user inputs from GUI
    try: 
        pressed_keys=[]
        def add_key(key): 
            if key not in pressed_keys: 
                pressed_keys.append(key)
        def remove_key(key): 
            if key in pressed_keys: 
                pressed_keys.remove(key)
        
        try:
            with pynput.keyboard.Listener(on_press = add_key, on_release = remove_key) as listener:
                for key in all_keys: # Populate default values
                    element = get_element(data_tree, key)
                    default = getattr(element, 'default', None)
                    if default is not None:
                        write_actuation(global_vars, key, default, "auto", do_print=False)
        
                init_event.set()
                print(f"[GUIHandler] started")
                while not stop_event.is_set():
                    try:
                        cmd = telemetry.command_queue.get(timeout=0.1)
                    except Empty:
                        continue
                    
                    pressed = all(k in pressed_keys for k in confirmation_keys)
                        
                    key, requested = cmd["key"], cmd["requested"]
        
                    element = get_element(data_tree, key)
                    if element is None:
                        print(f"[GUIHandler] Unknown GUI Key Error: '{key}'")
                        continue
                    
                    missing_keys = [k for k in confirmation_keys if k not in pressed_keys]
                    write_actuation(global_vars, key, requested, "user", do_print=print_switch_changes, pressed=pressed, missing_keys=missing_keys)
                        
                    
        except Exception as e:
            if not stop_event.is_set():
                _, _, tb = sys.exc_info()
                print(f"[GUIHandler] Initial Write Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
        finally: # write nominal states for all switches
            for key in all_keys: # Populate default values
                element = get_element(data_tree, key)
                default = getattr(element, 'nominal', None)
                if default is not None:
                    write_actuation(global_vars, key, default, "auto", do_print=False)
            pass
                
    except Exception as e:
            _, _, tb = sys.exc_info()
            '''if type(e).__name__ == 'StreamClosed'  or type(e).__name__ == 'ConnectionClosedError': # if synnax cluster stops unexpectedly
                if not non_abort_shutdown.is_set():
                    non_abort_shutdown.set()
            else:'''
            print(f"[GUIHandler] Setup error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
    finally: 
        print(f"[GUIHandler] stopped")


def main():
    try:
        # Check configs
        check_configs(global_vars)
        
        # Create servers
        openmct.start()
        servers.append(("OpenMCTServer", openmct))
        telemetry.start()
        servers.append(("TelemetryServer", telemetry))
        
        # Create Threads
        peripheral_init_event = threading.Event()
        peripheral_handler = threading.Thread(target=peripheral_worker, name="PeripheralHandler", args=(peripheral_init_event,))
        peripheral_handler.daemon = True
        peripheral_handler.start()
        threads.append(("PeripheralHandler", peripheral_handler))
        
        gui_init_event = threading.Event()
        gui_handler = threading.Thread(target=gui_worker, name="GUIHandler", args=(gui_init_event,))
        gui_handler.daemon = True
        gui_handler.start()
        threads.append(("GUIHandler", gui_handler))
        
        while not (peripheral_init_event.is_set() and gui_init_event.is_set()): # wait for all threads to initialize 
            time.sleep(.1)
        
        write_actuation(global_vars, peripherals['Valhala_I'].elements['flight_phase'].key, 'pad', "auto", do_print=False) ##### TEMP!!! #####
        
        print("\n\nPress Ctrl+C to stop...\n\n")
        while not stop_event.is_set():
            try:
                start = time.perf_counter()
                
                # Check for dead servers
                dead_servers = [(name, s) for name, s in servers if not s.is_running]
                if dead_servers:
                    print(f"[HealthCheck] Warning: Dead servers detected: {[name for name, _ in dead_servers]}")
                    
                # Check for dead threads
                dead_threads = [(name, t) for name, t in threads if not t.is_alive()]
                if dead_threads:
                    print(f"[HealthCheck] Warning: Dead threads detected: {[name for name, _ in dead_threads]}")
                
                # Wait for next interval
                while (time.perf_counter() - start < health_check_interval) and not stop_event.is_set():
                    if non_abort_shutdown.is_set(): # check for special conditions
                        if not stop_event.is_set():
                            shutdown(global_vars, do_abort=False)
                    time.sleep(.05)
                
            except Exception as e:
                _, _, tb = sys.exc_info()
                print(f"[HealthCheck] Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")# write initial display with settings
                time.sleep(1)
        if not stop_event.is_set():
            shutdown(global_vars)
        
    except KeyboardInterrupt as e:
        if not stop_event.is_set():
            shutdown(global_vars)

    except Exception as e:
        _, _, tb = sys.exc_info()
        print(f"[Main Thread] Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
        if not stop_event.is_set():
            shutdown(global_vars)

if __name__ == '__main__':
    main()

'''
COMPLETED:
    write_actuator called when actuator key is received in peripheral_worker
        Abort and shutdown peripheral logic
    Sequences supported by buttons, switches, and selectors
    Open Source License
    Moved write_actuators, abort, unabort, and shutdown to utility. Renamed utility to midgard_functions. Created global_vars tuple to pass global variables to operating functions and sequences
    

TO DO
    Bugs

    Configuration
        Check configurations are valid
            Warnings for certain config variables like debug_mode and keep_csv
            No duplicate ids of the same element class
            Check that peripheral ids are valid
            Check no duplicates in id arrays in toml files in _resolve_inheritance
            Check that no peripherals in python are using the same external device 
            Check that all the members of s sync group have the same values (except name, id, description, etc)
            Check sequences use a supported control_type
        Parent file from other manufacturer
        Armed and disarmed from another peripheral
    Overseer
        Restart of MIDGARD or peripheral handling
        Peripheral communication
        Install OpenMCT on first run (build_openmct.sh but thru python instead of bash)
    GUI
        Add cluster control_type
        Add inhibited view state
        Terminal Display
        Features
            Servo Alignment
            Restart frontend while python is running?
    Data Handling
        Recieving data from peripherals
        Data priority (bypass, gpio 24/25, diagnostics, etc)
        Import csv logs into database
    Actuator Control
        Actuating radio peripherals
        Add servo and cluster support to sequences
    Writing
        Readme
        How to
        Flight procedure
    Testing
        NI
        LabJack
    Other Peripherals
        MAVLink
        KSP (KSP OpenMCT and Telemachus Reborn https://gitlab.com/overloader-ksp/kerbal-telemetry)
        Ansys STK
        Basilisk (BSK)

'''