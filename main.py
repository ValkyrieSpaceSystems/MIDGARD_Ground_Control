import time, csv, os, threading, sys, shutil, re, tomllib, math, pynput, csv
import numpy as np
from queue import Queue, Empty, Full
from datetime import datetime, UTC

from peripheral import Peripheral
from openmct import OpenMCTServer, TelemetryServer, buildOpenMCTjs
from utility import get_element, combine_with_and

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

abort and logging actuators cannot have sources

'''


# Configuration Variables
debug_mode = True
show_server_logs = False
simulated_data = True
log_data = True # Boolean. Log data to csv
log_actuations = True # Boolean. Log GUI commands to csv
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
startup_event = threading.Event()

for name, args in peripherals.items():
    peripherals[name] = Peripheral(stop_event, name, args)

data_tree, all_keys = buildOpenMCTjs(peripherals)

openmct = OpenMCTServer(port=4000, show_logs=show_server_logs)
telemetry = TelemetryServer(stop_event, log_data=log_data, log_actuations=log_actuations, log_output_dir=log_output_dir, port=4001, show_logs=show_server_logs)


def write_actuation(key, requested, source, do_print=True, pressed=True, missing_keys=[]):
    element = get_element(data_tree, key)
    try:
        if not stop_event.is_set():
            if element.control_type == "switch":
                requested = bool(requested)
            
            # evaluate requested
            
            if element.control_type == "selector":
                current_state = element.states_index[element.value]
                requested_state = element.states_index[requested]
                valid_transition = True if requested_state in current_state.transition_to else False
                inhibited = any(not bool(elem.value) for elem in requested_state.armed_by) or any(bool(elem.value) for elem in requested_state.disarmed_by)
                
            else:
                valid_transition = True
                inhibited = any(not bool(elem.value) for elem in element.armed_by) or any(bool(elem.value) for elem in element.disarmed_by)
            
            valid_source = True if source in element.sources or element.sources == [] else False
            
            if not pressed or not valid_source or not valid_transition: # Stay the same
                value = element.value
            elif inhibited: # Nominalize
                value = element.nominal
            else: # Actuate
                value = requested
                
                
            # Global Logic
            if key.split('.')[-1].lower() == 'abort':
                if value:
                    abort('Digital Abort')
                elif not value:
                    unabort()
                
                for elem_key in all_keys: # set all other abort actuators 
                    if elem_key.split('.')[-1].lower() == 'abort' and elem_key != key:
                        elem = get_element(data_tree, elem_key)
                        if elem.element_type == 'Actuator':
                            telemetry.send(int(time.time() * 1000), elem.key, value, data_tree, source)
                            
            elif key.split('.')[-1].lower() == 'logging':
                if value:
                    telemetry.start_logging()
                elif not value:
                    telemetry.stop_logging()
                
                for elem_key in all_keys: # set all other logging actuators 
                    if elem_key.split('.')[-1].lower() == 'logging' and elem_key != key:
                        elem = get_element(data_tree, key)
                        if elem.element_type == 'Actuator':
                            telemetry.send(int(time.time() * 1000), elem.key, value, data_tree, source)
            
            
            if value == True:
                affect_array = element.disarms
            elif value == False:
                affect_array = element.arms
            elif element.control_type == 'selector':
                affect_array = element.states_index[value].disarms + element.states_index[element.value].arms 
                
            to_nominalize = [elem for elem in affect_array if elem.value == True] # list of all switches to nominalize with this switches actuation
                
            if to_nominalize:
                prop_complete = False
                while not prop_complete: # this populates the to_nominalize array with all the elements to be nominalized by the nominalizing of all the elements already in to_nominalize
                    start = to_nominalize
                    for e in to_nominalize:
                        to_nominalize = to_nominalize + [elem for elem in e.arms if elem not in to_nominalize and elem.value == True]
                    if to_nominalize == start: prop_complete = True                    
                for elem in to_nominalize: # Nominalize the elements
                    if elem.element_type == 'State':
                        par = elem.parent
                        telemetry.send(int(time.time() * 1000), par.key, par.nominal, data_tree, source)
                    else:
                        telemetry.send(int(time.time() * 1000), elem.key, elem.nominal, data_tree, source)
                    
                    
                    ##############
                    # Stop Sequences
                    ##############
                    
            
            telemetry.send(int(time.time() * 1000), element.key, value, data_tree, source)
            
            
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
                if to_nominalize:
                    print(f'{element.name} now inhibiting and setting nominal {[elem.name for elem in to_nominalize]}')
                    
                if pressed and valid_source and valid_transition:
                    if element.control_type == "switch":
                        if element.value == False:
                            display_value = element.nominal_state
                        else:
                            display_value = element.off_nominal_state
                    elif element.control_type == "selector":
                        display_value = element.states_name[value]
                        inhibited = any(not bool(elem.value) for elem in requested_state.armed_by) or any(bool(elem.value) for elem in requested_state.disarmed_by)
                    else:
                        display_value = value
                        
                    if element.control_type == "button":
                        control_type_display = f"triggered"
                    else:
                        control_type_display = f"set to {display_value}"
                                            
                    if not inhibited:
                        print(f"{element.name} {control_type_display}")
                    elif inhibited:
                        if element.control_type == 'selector':
                            blocked = [elem.name for elem in requested_state.armed_by if elem.value == False] + [elem.name for elem in requested_state.disarmed_by if elem.value == True]  # A list of all inhbiiting elements
                        else:
                            blocked = [elem.name for elem in element.armed_by if elem.value == False] + [elem.name for elem in element.disarmed_by if elem.value == True]  # A list of all inhbiiting elements
                        print(f"{element.name} {control_type_display}. Inhibited by {blocked}")
                
                else: 
                    reasons = []
                    if not pressed: # Confirmation keys not pressed
                        missing_key_names = []
                        for k in missing_keys:
                            if isinstance(k, str):
                                missing_key_names.append(k.upper())
                            elif isinstance(k, pynput.keyboard.Key):
                                missing_key_names.append(k.name.replace('_', ' ').title())
                        
                        reasons.append(f"{missing_key_names} not pressed")
                        
                    if not valid_source:
                        reasons.append(f"{source} not valid source {element.sources}")
                        
                    if not valid_transition:
                        reasons.append(f"{requested_state.name} not valid transition path {[s.name for s in current_state.transition_to]}")
                        
                    print(f"{element.name} command ignored because {combine_with_and(reasons)}")
                
            
            ##############
            # Perform Triggers and Sequences
            ##############
            if element.type == "trigger" and pressed and not inhibited and valid_source:
                print('TRIGGER')
            
                    
    except Exception as e:
        _, _, tb = sys.exc_info()
        print(f"{element.name} Write Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")

def abort(cause='', verbose_cause=True, verbose_abort=True): # A digital abort exists, but cant be activated without an 'abort' switch (except during shutdown)
    if not abort_state.is_set():
        try: 
            abort_state.set() # sets first to stop anything else from writing and interfering with abort
            
            ###
            # Peripheral Abort Logic
            ###
                    
            if verbose_abort: 
                print("\nABORTING\nABORTING\nABORTING\n")
            if cause != '' and verbose_cause:
                print(f'Abort: {cause}\n')
        except Exception as e:
            _, _, tb = sys.exc_info()
            print(f"Abort Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")

def unabort(verbose=True): 
    if abort_state.is_set():
        if verbose:
            print("\nUnaborted\n")
        abort_state.clear() # This just unsets the abort_state event

def shutdown(do_abort=True):
    print("\n\n\nStopping MIDGARD...")
    stop_event.set() # Runs this to stop everything else from running and to enable the shutdown process
    try:
        if do_abort: # this is the propper shutdown
            try: # Ensure all relays are turned off on exit
                abort('Shut Down', True, False)
            except Exception as e:
                _, _, tb = sys.exc_info()
                print(f'Error Closing Valves: {type(e).__name__} on line {tb.tb_lineno}: {e}')
            time.sleep(.1) # Short wait for NI, caused error when trying to close last ssr task without delay
        else:
            abort_state.set() # This is for special error shutdowns such as the synna cluster stopping while this program is running
        
        safe = True
        unsafe = []
        
        # Close peripherals
        write_actuation(peripherals['Valhala_I'].elements['flight_phase'].key, -1, "auto", do_print=False)
        
        for name, server in servers: # stops all running threads (runs after closing ni tasks because threads call ni tasks while running, would cause an error if reversed order)
            server.stop(delete_db=True)
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
                
        if not save_csv: # Removes csv logs if that setting is set
            print('[Shutdown] removing logs')
            for file in telemetry.csv_files:
                try:
                    os.remove(file)
                except FileNotFoundError:
                    pass
                except Exception as e:
                    print(f'Failed to remove file {file}: {type(e).__name__} on line {tb.tb_lineno}: {e}')
                
                
    except Exception as e:
        _, _, tb = sys.exc_info()
        print(f"[Shutdown] Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
            
    if safe: print("\n\n[Shutdown] All systems stopped safely\n\n")
    else: print(f"\n\n[Shutdown] Threads or servers did not stop safely: {unsafe}\n\n")
            

def incoming_data_handler(init_event):
    try:
        if simulated_data:
            i = 0
            j = False
            str_list = ['a','b','c','d','e','f','g','h','i','j','k']
            
        init_event.set()
        print(f"[DataHandler] started")
        try:
            while not stop_event.is_set():
                if simulated_data:
                    data = {}
                    for key in all_keys:
                        element = get_element(data_tree, key)
                        if element.element_type == 'Data_Stream':
                            if element.unit == 'bool': data[key] = j
                            elif element.unit == 'str': data[key] = str_list[math.floor(i/10)]
                            else: data[key] = i
                    i = 0 if i == 100 else i + 1
                    j = not j if i == 100 else j
                    utc = int(time.time() * 1000)
                else:
                    pass
                    # scale and offset
                    
                #print(data)
                
                for stream_id, value in data.items():
                    telemetry.send(utc, stream_id, value, data_tree, "peripheral")
                    
                    '''entry = data_tree.get(key)
                    if entry is None:
                        print(f"[MIDGARD WARNING] Unrecognized telemetry key '{key}'")
                        continue
                    peripheral, stream_id = entry
                    peripheral.update_data(stream_id, value)   # sets .value, checks in_range(), alerts
                    telemetry.send(key, value)'''
                    
                    
                    '''
                ds = self.Data Streams.get(stream_id)
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
        print(f"[DataHandler] stopped")
        
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
                    element = get_element(data_tree, key)
                    default = getattr(element, 'default', None)
                    if default is not None and element.control_type != "trigger":
                        write_actuation(key, default, "auto", do_print=False)
        
                init_event.set()
                print(f"[CommandHandler] started")
                while not stop_event.is_set():
                    try:
                        cmd = telemetry.command_queue.get(timeout=0.1)
                    except Empty:
                        continue
                    
                    pressed = all(k in pressed_keys for k in confirmation_keys)
                        
                    key, requested = cmd["key"], cmd["requested"]
        
                    element = get_element(data_tree, key)
                    if element is None:
                        print(f"[CommandHandler] Unknown GUI Key Error: '{key}'")
                        continue
                    
                    missing_keys = [k for k in confirmation_keys if k not in pressed_keys]
                    write_actuation(key, requested, "user", do_print=print_switch_changes, pressed=pressed, missing_keys=missing_keys)
                        
                    
        except Exception as e:
            if not stop_event.is_set():
                _, _, tb = sys.exc_info()
                print(f"[CommandHandler] Initial Write Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
        finally: # write nominal states for all switches
            for key in all_keys: # Populate default values
                element = get_element(data_tree, key)
                default = getattr(element, 'nominal', None)
                if default is not None:
                    write_actuation(key, default, "auto", do_print=False)
            pass
                
    except Exception as e:
            _, _, tb = sys.exc_info()
            '''if type(e).__name__ == 'StreamClosed'  or type(e).__name__ == 'ConnectionClosedError': # if synnax cluster stops unexpectedly
                if not non_abort_shutdown.is_set():
                    non_abort_shutdown.set()
            else:'''
            print(f"[CommandHandler] Setup error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
    finally: 
        print(f"[CommandHandler] stopped")


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
        
        write_actuation(peripherals['Valhala_I'].elements['flight_phase'].key, 1, "auto", do_print=False)
        
        print("\n\nPress Ctrl+C to stop...\n\n")
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
    Selector
    Make units optional
    Shutdown sequence (Abort and peripheral close)
    Logging data and commands (link all actuators with id logging)
    Global abort (link all actuators with id abort)
    

TO DO
    Configuration
        Check configurations are valid
        Remove an item from a parent id file (ie remove id+pyro_motor from ASGARD_V0.2.toml)
        Sequence handling
        Parent from other manufacturer
    Overseer
        Error handling
        Event handling 
        Restart of MIDGARD or peripheral handling
        Peripheral communication
            Abort and shutdown peripheral logic
        Install OpenMCT on first run (build_openmct.sh but thru python instead of bash)
    GUI
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
        Actuating
        Sequences (sequences prevet the user from using the actuators the sequence use while active)
        Triggers (sequences prevet the user from using the actuators the sequence use while active)
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