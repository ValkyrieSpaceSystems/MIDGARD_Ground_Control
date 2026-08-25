import time, csv, os, threading, sys, shutil, re, tomllib, math, pynput
import numpy as np
from queue import Queue, Empty, Full
from datetime import datetime, UTC

from peripheral import Peripheral
from openmct import OpenMCTServer, TelemetryServer, buildOpenMCTjs
from utility import get_element

import nidaqmx
from nidaqmx import stream_readers, DaqReadError
from nidaqmx.stream_writers import DigitalSingleChannelWriter, DigitalMultiChannelWriter
from nidaqmx.constants import TerminalConfiguration, AcquisitionType, LineGrouping, WAIT_INFINITELY
import nidaqmx.system
from labjack import ljm
import Basilisk


'''
peripheral folder containing folders for each manufacturer, each manufacturer has files for each ID containing information about the sensors (raw data), processed data, actuators, and other variables. each system and model has a gerneric file defining system and model specific variables and default values and telling MIDGARD how to communicate with this specific device on different interfaces and what interfaces are valid (optional interfaces will be defined in the other variables setting). system folders can also contain custom sequences for that system and id files can contain whether they're valid

peripherals are made of elements (data streams, actuators, lockouts, interfaces, and phases)


openmct was built with npm run build and is being served by fastapi through python. sqlite is the telemetry server serving realtime and historical data. 

element id and type cannot be the same (ie id = "heading" type = "heading")

toml default is the state midgard writes automatically at startup, a value of false is the nominal/safe position (ie normally open or normally closed is the safe state and configured in hardware to equate with a value of false)

'''


# Configuration Variables
debug_mode = True
show_server_logs = False
simulated_data = True
log_data = True # Boolean. Log data to csv
log_commands = True # Boolean. Log GUI commands to csv
print_switch_changes = True # Boolean. Print switch state changes to python terminal
save_csv = False # Boolean. If False, deletes csv files after run. Should only be False in testing
log_output_dir = '' # String of absolute file path. Directory where to store log files. Will attempt to make directory if it doesn't already exist. If left blank (ie ''), will use logs folder inside current working directory.
confirmation_keys = [pynput.keyboard.Key.shift] # List of single character strings for keys or pynput.keyboard.Key for which keys to press to enable switch actuation. Uses AND logic. Leave empty for no confirmation [DANGEROUS!!!]


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

for name, args in peripherals.items():
    peripherals[name] = Peripheral(stop_event, name, args)

data_tree, all_keys = buildOpenMCTjs(peripherals)

openmct = OpenMCTServer(stop_event, port=4000, show_logs=show_server_logs)
telemetry = TelemetryServer(stop_event, port=4001, show_logs=show_server_logs)


def write_actuation(key, element, requested, source, do_print=print_switch_changes, pressed=True, missing_keys=None):
    try:
        if not stop_event.is_set():
            if element.unit == "bool":
                requested = bool(requested)
            
            #print(key,element.armed_by,element.disarmed_by)
            
            element = get_element(data_tree, key.split('.'))
            # evaluate requested
            inhibited = any(not bool(elem.value) for elem in element.armed_by) or any(bool(elem.value) for elem in element.disarmed_by)
            if not pressed:
                value = element.value
            elif inhibited: 
                value = element.nominal
            else: 
                value = requested
            
            if value == True:
                affect_array = element.disarms
            elif value == False:
                affect_array = element.arms
                
            to_nominalize = [elem for elem in affect_array if elem.value == True] # list of all switches to nominalize with this switches actuation
            prop_complete = False
            while not prop_complete: # this populates the to_nominalize array with all the elements to be nominalized by the nominalizing of all the elements already in to_nominalize
                start = to_nominalize
                for switch in to_nominalize:
                    to_nominalize = to_nominalize + [elem for elem in element.arms if elem not in to_nominalize and elem.value == True]
                if to_nominalize == start: prop_complete = True
                
            if to_nominalize:
                if do_print: 
                    print(f'{element.name} now inhibiting and setting nominal {[elem.name for elem in to_nominalize]}')
                for elem in to_nominalize: # Nominalize the elements
                    telemetry.send(elem.key, elem.value, data_tree, source, push_to_gui=False) 
                    telemetry.send(elem.key, elem.nominal, data_tree, source)
                    
                    
                    ##############
                    # Stop sequences
                    ##############
                    
            
            # done twice to show actuation on a graph as a step instead of a long slope
            telemetry.send(element.key, element.value, data_tree, source, push_to_gui=False) 
            telemetry.send(element.key, value, data_tree, source)
            
            
            ##############
            # Perform Actuation
            ##############
            if element.parent.manufacturer == "VSS":
                pass
            elif element.parent.manufacturer == "NI":
                pass
            elif element.parent.manufacturer == "LabJack":
                pass
            
            
            if do_print: 
                if pressed:
                    if element.unit == "bool":
                        if element.value == False:
                            display_value = element.nominal_state
                        else:
                            display_value = element.off_nominal_state
                    else:
                        display_value = value
                    if not inhibited:
                        print(f"{element.name} set to {display_value}")
                    elif inhibited:
                        blocked = [elem.name for elem in element.armed_by if elem.value == False] + [elem.name for elem in element.disarmed_by if elem.value == True]  # A list of all inhbiiting elements
                        print(f"{element.name} set to {display_value}. Inhibited by {blocked}")
                else: # Confirmation keys not pressed
                    missing_key_names = []
                    for k in missing_keys:
                        if isinstance(k, str):
                            missing_key_names.append(k.upper())
                        elif isinstance(k, pynput.keyboard.Key):
                            missing_key_names.append(k.name.replace('_', ' ').title())
                    print(f"{element.name} command ignored because {missing_key_names} not pressed")
                    
    except Exception as e:
        _, _, tb = sys.exc_info()
        print(f"{element.name} Write Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")


def shutdown(do_abort=True):
    print("\nStopping MIDGARD...")
    stop_event.set() # Runs this to stop everything else from running and to enable the shutdown process
    try:
    
        ## Abort logic
        
        safe = True
        unsafe = []
        
        # Close peripherals
        
        for name, server in servers: # stops all running threads (runs after closing ni tasks because threads call ni tasks while running, would cause an error if reversed order)
            server.stop()
            if server.is_running:
                print(f"[Shutdown] Warning: {name} server did not stop cleanly")
                safe = False
                unsafe.append(name)
                
        for name, thread in threads: # stops all running threads (runs after closing ni tasks because threads call ni tasks while running, would cause an error if reversed order)
            thread.join(timeout=5)
            if thread.is_alive():
                print(f"[Shutdown] Warning: {name} server did not stop cleanly")
                safe = False
                unsafe.append(name)
                
    except Exception as e:
        _, _, tb = sys.exc_info()
        print(f"[Shutdown] Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
            
    if safe: print("[Shutdown] All systems stopped safely\n\n")
    else: print(f"[Shutdown] Threads or servers did not stop safely: {unsafe}\n\n")
            

def incoming_data_handler(init_event):
    try:
        if simulated_data:
            i = 0
            j = False
            str_list = ['a','b','c','d','e','f','g','h','i','j','k']
            
        init_event.set()
        try:
            while not stop_event.is_set():
                if simulated_data:
                    data = {}
                    for key in all_keys:
                        element = get_element(data_tree, key.split('.'))
                        if element.element_type == 'Data_Stream':
                            if element.unit == 'bool': data[key] = j
                            elif element.unit == 'str': data[key] = str_list[math.floor(i/10)]
                            else: data[key] = i
                    i = 0 if i == 100 else i + 1
                    j = not j if i == 100 else j
                else:
                    pass
                    # scale and offset
                    
                #print(data)
                
                for stream_id, value in data.items():
                    telemetry.send(stream_id, value, data_tree, "temp_source")
                    
                    '''entry = data_tree.get(key)
                    if entry is None:
                        print(f"[MIDGARD WARNING] Unrecognized telemetry key '{key}'")
                        continue
                    peripheral, stream_id = entry
                    peripheral.update_data(stream_id, value)   # sets .value, checks in_range(), alerts
                    telemetry.send(key, value)'''
                    
                    
                    '''
                ds = self.data_streams.get(stream_id)
                if ds is None:
                    print(f"[MIDGARD WARNING] Unknown data stream '{stream_id}'")
                    return
                ds.value = value
                #if not ds.in_range():
                    #print(f"[MIDGARD ALERT] {stream_id} = {value} {ds.unit} out of range {ds.range}")'''
                
                time.sleep(0.1)
                
        except Exception as e:
            _, _, tb = sys.exc_info()
            print(f"[DataHandler] Read Error: {type(e).__name__} on line {tb.tb_lineno}: {e}") 
                
    except Exception as e:
        if not stop_event.is_set():
            _, _, tb = sys.exc_info()
            print(f"[DataHandler] Setup Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
    finally: 
        print(f"[DataHandler] Thread Stopped")
        

def incoming_command_handler(init_event):
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
                    element = get_element(data_tree, key.split('.'))
                    default = getattr(element, 'default', None)
                    if default is not None:
                        write_actuation(key, element, default, "Startup", do_print=False)
        
                init_event.set()
                while not stop_event.is_set():
                    try:
                        cmd = telemetry.command_queue.get(timeout=0.1)
                    except Empty:
                        continue
                    
                    pressed = all(k in pressed_keys for k in confirmation_keys)
                        
                    
                    key, requested = cmd["key"], cmd["requested"]
                    element = get_element(data_tree, key.split('.'))
                    if element is None:
                        print(f"[CommandHandler] Unknown GUI Key Error: '{key}'")
                        continue
                    
                    missing_keys = [k for k in confirmation_keys if k not in pressed_keys]
                    write_actuation(key, element, requested, "Temp_Source", pressed=pressed, missing_keys=missing_keys)
                        
                    
        except Exception as e:
            if not stop_event.is_set():
                _, _, tb = sys.exc_info()
                print(f"[CommandHandler] Initial Write Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
        finally: # write nominal states for all switches
            for key in all_keys: # Populate default values
                element = get_element(data_tree, key.split('.'))
                default = getattr(element, 'default', None)
                if default is not None:
                    write_actuation(key, element, default, "Shutdown", do_print=False)
            pass
                
    except Exception as e:
            _, _, tb = sys.exc_info()
            '''if type(e).__name__ == 'StreamClosed'  or type(e).__name__ == 'ConnectionClosedError': # if synnax cluster stops unexpectedly
                if not non_abort_shutdown.is_set():
                    non_abort_shutdown.set()
            else:'''
            print(f"[CommandHandler] Setup error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
    finally: 
        print(f"[CommandHandler] Thread Stopped")

def main():
    try:
        # Create servers
        global openmct
        openmct.start()
        servers.append(("OpenMCTServer", openmct))
        global telemetry
        telemetry.start()
        servers.append(("TelemetryServer", telemetry))
        
        # Create Threads
        data_init_event = threading.Event()
        data_handler = threading.Thread(target=incoming_data_handler, name="DataHandler", args=(data_init_event,))
        data_handler.daemon = True
        data_handler.start()
        threads.append(("DataHandler", data_handler))
        
        command_init_event = threading.Event()
        command_handler = threading.Thread(target=incoming_command_handler, name="CommandHandler", args=(command_init_event,))
        command_handler.daemon = True
        command_handler.start()
        threads.append(("CommandHandler", command_handler))
        
        while not (data_init_event.is_set() and command_init_event.is_set()): # wait for all threads to initialize 
            time.sleep(.1)
    
        print("\nPress Ctrl+C to stop...\n\n")
        interval = 5 # Check every x seconds
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
                while (time.perf_counter() - start < interval) and not stop_event.is_set():
                    if non_abort_shutdown.is_set(): # check for special conditions
                        if not stop_event.is_set():
                            shutdown(do_abort=False)
                    time.sleep(.05)
                
            except Exception as e:
                _, _, tb = sys.exc_info()
                print(f"[HealthCheck] Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")# write initial display with settings
                time.sleep(1)
        if not stop_event.is_set():
            shutdown()
        
    except KeyboardInterrupt as e:
        if not stop_event.is_set():
            shutdown()

    except Exception as e:
        _, _, tb = sys.exc_info()
        print(f"[Main Thread] Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
        if not stop_event.is_set():
            shutdown()

if __name__ == '__main__':
    main()

'''
COMPLETED:
    Recieving commands from GUI, checking conditions, sending affirm/deny
    Watchdog
    Switches

    

TO DO
    Configuration
        Create data structures
        Check configurations are valid
        Remove an item from a parent id file (ie remove id+pyro_motor from ASGARD_V0.2.toml)
        Sequence handling
        Parent from other manufacturer
    Overseer
        Error handling
        Event handling 
        Shutdown sequence (Abort and peripheral close)
        Restart of MIDGARD or peripheral handling
        Peripheral communication
    GUI
        Make units optional for some element types
        Multi-Select
        Add inhibited view state
        Terminal Display
        Features
            Servo Alignment
            Restart frontend while python is running?
    Data Handling
        Recieving data
        Logging data (export database to csv?)
        Low priority data (bypass, gpio 24/25, diagnostics, etc)
    Actuator Control
        Actuating
    Writing
        Readme
        How to
        Flight procedure
        Open Source License
    Other Peripherals
        NI
        LabJack
        KSP (KSP OpenMCT and Telemachus Reborn https://gitlab.com/overloader-ksp/kerbal-telemetry)
        Ansys STK
        Basilisk (BSK)

'''