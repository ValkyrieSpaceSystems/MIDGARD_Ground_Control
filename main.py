import time, csv, os, threading, sys, shutil, re, tomllib, math, pynput, csv
import numpy as np
from queue import Queue, Empty, Full
from datetime import datetime, UTC

from peripheral import Peripheral
from openmct import OpenMCTServer, TelemetryServer, buildOpenMCTjs
from utility import get_element, combine_with_and, label

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

for NI each cDAQ is a single peripheral


openmct was built with npm run build and is being served by fastapi through python. sqlite is the telemetry server serving realtime and historical data. 

toml default is the state midgard writes automatically at startup, a value of false is the nominal/safe position (ie normally open or normally closed is the safe state and configured in hardware to equate with a value of false)

abort and logging actuators cannot have sources

if id and type are the same, the type folder will collapse and become the element object. if id and type are the same, no other elements of that type will be supported for that peripheral

cannot have duplicate ids of the same element class

'''


# Configuration Variables
debug_mode = True
show_server_logs = False
simulated_data = True
log_data = True # Boolean. Log data to csv
log_actuations = True # Boolean. Log GUI commands to csv
print_switch_changes = True # Boolean. Print switch state changes to python terminal
keep_csv = False # Boolean. If False, deletes csv files after run. Should only be False in testing
openmct_dir = '' # String of absolute file path. Directory where OpenMCT is installed. Will attempt to install if it doesn't already exist. If left blank (ie ''), will use openmct folder inside current working directory.
log_output_dir = '' # String of absolute file path. Directory where to store log files. Will attempt to make directory if it doesn't already exist. If left blank (ie ''), will use logs folder inside current working directory.
confirmation_keys = [pynput.keyboard.Key.shift] # List of single character strings for keys or pynput.keyboard.Key for which keys to press to enable switch actuation. Uses AND logic. Leave empty for no confirmation [DANGEROUS!!!]


peripherals = {
    'Valhala_I':{'interface':'elrs', 'manufacturer':'VSS', 'id':'ASGARD_V0.2'},
    #'nidaq':{'interface':'ethernet', 'manufacturer':'NI', 'id':'1'},
    #'labjack':{'interface':'usb', 'manufacturer':'LabJack', 'id':'1'},
}

# check user configs

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


def write_actuation(key, requested, source, do_print=True, pressed=True, missing_keys=[], bypass_checks=False):
    element = get_element(data_tree, key)
    try:
        if not stop_event.is_set():
            to_actuate = []
            
            if element.control_type == "switch":
                requested = bool(requested)
            elif element.control_type == "selector":
                if isinstance(requested, str):
                    requested = element.states[requested].index
            
            
            # Evaluation of Request
            if element.control_type == "selector":
                current_state = element.states_index[element.value]
                requested_state = element.states_index[requested]
                valid_source = True if source in requested_state.sources or requested_state.sources == [] else False
                valid_transition = True if requested_state in current_state.transition_to else False
                inhibited = any(not bool(elem.value) for elem in requested_state.armed_by) or any(bool(elem.value) for elem in requested_state.disarmed_by)
            else:
                valid_transition = True
                valid_source = True if source in element.sources or element.sources == [] else False
                inhibited = any(not bool(elem.value) for elem in element.armed_by) or any(bool(elem.value) for elem in element.disarmed_by)
                
            if bypass_checks:
                pressed = True
                valid_source = True
                valid_transition = True
                inhibited = False
            
            if not pressed or not valid_source or not valid_transition: # Stay the same
                value = element.value
            elif inhibited: # Nominalize
                value = element.nominal
            else: # Actuate
                value = requested
                
                
            # Sync Groups
            if element.sync_group == 'abort':
                if value:
                    abort('Digital Abort')
                elif not value:
                    unabort()
            
            elif element.sync_group == 'logging':
                if value:
                    telemetry.start_logging()
                elif not value:
                    telemetry.stop_logging()
                
            if element.sync_group:
                for elem in sync_groups.get(element.sync_group, []):
                    if elem is not element:
                        telemetry.send(int(time.time() * 1000), elem.key, value, data_tree, source)
                        to_actuate.append(elem)
                 
                 
            # Normalization
            if value == True:
                affect_array = element.disarms
            elif value == False:
                affect_array = element.arms
            elif element.control_type == 'selector':
                affect_array = element.states_index[value].disarms + element.states_index[element.value].arms 
                
            to_nominalize = [elem for elem in affect_array if elem.value == True and elem.control_type != 'button'] # list of all switches to nominalize with this switches actuation
                
            if to_nominalize:
                prop_complete = False
                while not prop_complete: # this populates the to_nominalize array with all the elements to be nominalized by the nominalizing of all the elements already in to_nominalize
                    start = to_nominalize
                    for e in to_nominalize:
                        to_nominalize = to_nominalize + [elem for elem in e.arms if elem not in to_nominalize and elem.value == True and elem.control_type != 'button']
                    if to_nominalize == start: prop_complete = True                    
                for elem in to_nominalize: # Nominalize the elements
                    if elem.element_class == 'State':
                        par = elem.parent
                        telemetry.send(int(time.time() * 1000), par.key, par.nominal, data_tree, source, immediate=True)
                        to_actuate.append(par)
                    if elem.type == 'sequence':
                        elem.stop_event.set()
                        if elem.thread != None:
                            elem.thread.join()
                            threads.remove((elem.name, elem.thread))
                            elem.thread = None
                        telemetry.send(int(time.time() * 1000), elem.key, elem.nominal, data_tree, source, immediate=True)
                        to_actuate.append(elem)
                    else:
                        telemetry.send(int(time.time() * 1000), elem.key, elem.nominal, data_tree, source, immediate=True)
                        to_actuate.append(elem)
                 
                 
            # Triggers and Sequences
            if element.type == "trigger" and pressed and not inhibited and valid_source:
                element.func(stop_event,element)
            elif element.type == 'sequence':
                if value:
                    element.stop_event.clear()
                    if element.thread == None:
                        element.thread = threading.Thread(target=element.func, args=(stop_event,element), daemon=True)
                        element.thread.start()
                        threads.append((element.name, element.thread))
                else:
                    element.stop_event.set()
                    if element.thread != None:
                        element.thread.join()
                        threads.remove((element.name, element.thread))
                        element.thread = None

            
            telemetry.send(int(time.time() * 1000), element.key, value, data_tree, source, immediate=True)
            to_actuate.append(element)
            
            
            # Physical Actuation
            write_ni = False
            ni_peripherals = []
            for elem in to_actuate:
                if elem.type in ['valve','ssr','servo','pyro']:
                    if elem.parent.manufacturer == "VSS":
                        pass
                    elif elem.parent.manufacturer == "NI":
                        write_ni = True
                        if elem.parent not in ni_peripherals:
                            ni_peripherals.append(elem.parent)
                    elif elem.parent.manufacturer == "LabJack":
                        ljm.eWriteName(elem.parent.handle, elem.channel, elem.value)
            if write_ni:
                for parent in ni_peripherals:
                    for module in parent.ni_modules:
                        if module.writer != None:
                            module.writer.write_one_sample_one_line(np.array([[elem.value if elem is not None else False for elem in module.channels]]))
                
            
            # Printing
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
                    elif element.type == "sequence":
                        control_type_display = f"{display_value}"
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
                    
                    if element.control_type == 'selector':
                        if not valid_source:
                            reasons.append(f"{source} not valid source {requested_state.sources}")
                            
                        if not valid_transition:
                            reasons.append(f"{requested_state.name} not valid transition {[s.name for s in current_state.transition_to]}")
                    else:
                        if not valid_source:
                            reasons.append(f"{source} not valid source {element.sources}")
                        
                    print(f"{element.name} command ignored because {combine_with_and(reasons)}")

                    
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
            for peripheral in peripherals.values():
                if peripheral.manufacturer == 'VSS':
                    pass
                
                elif peripheral.manufacturer == 'NI':
                    for writer in peripheral.writers: # Iterates through all writers and sets all channels on each module to false
                        writer.write_one_sample_one_line(np.array([False]*8))
                    
                elif peripheral.manufacturer == 'LabJack':
                    ljm.eWriteNames(peripheral.handle, len(peripheral.labjack_actuators), [elem.channel for elem in peripheral.labjack_actuators], [0]*len(peripheral.labjack_actuators))
                    
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
                abort('Shut Down', False, False)
            except Exception as e:
                _, _, tb = sys.exc_info()
                print(f'Error Closing Valves: {type(e).__name__} on line {tb.tb_lineno}: {e}')
            time.sleep(.1) # Short wait for NI, caused error when trying to close last ssr task without delay
        else:
            abort_state.set() # This is for special error shutdowns such as the synna cluster stopping while this program is running
        
        safe = True
        unsafe = []
        
        # Close peripherals
        write_actuation(peripherals['Valhala_I'].elements['flight_phase'].key, 'shutdown', "auto", do_print=False)
        
        for peripheral in peripherals.values():
            if peripheral.manufacturer == 'VSS':
                try: 
                    pass
                except Exception as e:
                    _, _, tb = sys.exc_info()
                    print(f'[Shutdown] Error: {peripheral.display_name}: {type(e).__name__} on line {tb.tb_lineno}: {e}')
            
            elif peripheral.manufacturer == 'NI':
                closed_tasks = []
                for module in peripheral.ni_modules.values(): # Closes all the NI tasks
                    try:
                        module.task.close()
                        closed_tasks.append(f'{module.name} ({module.module_number})')
                    except Exception as e:
                        _, _, tb = sys.exc_info()
                        print(f'[Shutdown] Error: {peripheral.display_name} NI Task for module {module.name} ({module.module_number}): {type(e).__name__} on line {tb.tb_lineno}: {e}')
                print(f'[Shutdown] Closed NI tasks for {combine_with_and(closed_tasks)}')
                
            elif peripheral.manufacturer == 'LabJack':
                try: # closes the labjack handle
                    ljm.close(peripheral.handle)
                except Exception as e:
                    _, _, tb = sys.exc_info()
                    print(f'[Shutdown] Error: {peripheral.display_name} LabJack did not close properly: {type(e).__name__} on line {tb.tb_lineno}: {e}')
                    
        
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
                
        if not keep_csv: # Removes csv logs if that setting is set
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


def peripheral_handler_worker(init_event):
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
                    i = 0 if i == 100 else i + 1
                    j = not j if i == 100 else j
                    ts = int(time.time() * 1000)
                    data = {ts:sample}
                else:
                    for peripheral in peripherals.values():
                        if peripheral.manufacturer == 'VSS':
                            pass
                        
                        elif peripheral.manufacturer == 'NI':
                            try:
                                data = peripheral.data_queue.get(timeout=1.0)
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
                                

                #print(data)
                max_utc = max(data)
                for utc, sample in sorted(data.items()):
                    for stream_key, value in sample.items():
                        if utc == max_utc:
                            telemetry.send(utc, stream_key, value, data_tree, "peripheral")
                        else:
                            telemetry.send(utc, stream_key, value, data_tree, "peripheral", push_to_gui=False)
                
        except Exception as e:
            _, _, tb = sys.exc_info()
            print(f"[PeripheralHandler] Read Error: {type(e).__name__} on line {tb.tb_lineno}: {e}") 
                
    except Exception as e:
        if not stop_event.is_set():
            _, _, tb = sys.exc_info()
            print(f"[PeripheralHandler] Setup Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
    finally: 
        print(f"[PeripheralHandler] stopped")
        
def gui_handler_worker(init_event):
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
                        write_actuation(key, default, "auto", do_print=False)
        
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
                    write_actuation(key, requested, "user", do_print=print_switch_changes, pressed=pressed, missing_keys=missing_keys)
                        
                    
        except Exception as e:
            if not stop_event.is_set():
                _, _, tb = sys.exc_info()
                print(f"[GUIHandler] Initial Write Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
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
            print(f"[GUIHandler] Setup error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
    finally: 
        print(f"[GUIHandler] stopped")


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
        peripheral_init_event = threading.Event()
        peripheral_handler = threading.Thread(target=peripheral_handler_worker, name="PeripheralHandler", args=(peripheral_init_event,))
        peripheral_handler.daemon = True
        peripheral_handler.start()
        threads.append(("PeripheralHandler", peripheral_handler))
        
        gui_init_event = threading.Event()
        gui_handler = threading.Thread(target=gui_handler_worker, name="GUIHandler", args=(gui_init_event,))
        gui_handler.daemon = True
        gui_handler.start()
        threads.append(("GUIHandler", gui_handler))
        
        while not (peripheral_init_event.is_set() and gui_init_event.is_set()): # wait for all threads to initialize 
            time.sleep(.1)
        
        write_actuation(peripherals['Valhala_I'].elements['flight_phase'].key, 'pad', "auto", do_print=False) ##### TEMP!!! #####
        
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
    Remove an item from a parent id file (ie remove id+pyro_motor from ASGARD_V0.2.toml)
    NI
    LabJack
    

TO DO
    Bugs

    Configuration
        Check configurations are valid
            Warnings for certain config variables like debug_mode and keep_csv
            No duplicate ids of the same element class
            Check that peripheral ids are valid
            Check no duplicates in id arrays in toml files in _resolve_inheritance
            Check that no peripherals in python are using the same external device 
        Parent file from other manufacturer
    Overseer
        Restart of MIDGARD or peripheral handling
        Peripheral communication
            Abort and shutdown peripheral logic
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
        Actuating
    Writing
        Readme
        How to
        Flight procedure
        Open Source License
    Testing
        NI
        LabJack
    Other Peripherals
        MAVLink
        KSP (KSP OpenMCT and Telemachus Reborn https://gitlab.com/overloader-ksp/kerbal-telemetry)
        Ansys STK
        Basilisk (BSK)

'''