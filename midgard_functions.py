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



# Helper Functions
def get_element(data_tree, key, default=None):
    current = data_tree
    for key_part in key.split('.'):
        if isinstance(current, dict) and key_part in current:
            current = current[key_part]
        else:
            return default
    #print(current)
    return current

def combine_with_and(items, oxford_comma=True):
    items = list(items)

    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"

    *head, last = items
    sep = "," if oxford_comma else ""
    return f"{', '.join(head)}{sep} and {last}"
    
def label(raw):
    return raw.replace('_', ' ').title()



# Operating Functions
def check_configs(global_vars):
    pass

def write_actuation(global_vars, key, requested, source, do_print=True, pressed=True, missing_keys=[], bypass_checks=False):
    debug_mode, show_server_logs, simulated_data, log_data, log_actuations, print_switch_changes, keep_csv, openmct_dir, log_output_dir, confirmation_keys, peripherals, threads, servers, stop_event, abort_state, non_abort_shutdown, startup_event, sync_groups, data_tree, all_keys, openmct, telemetry = global_vars
    
    element = get_element(data_tree, key)
    func_args = (global_vars, element)
    
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
                    abort(global_vars, 'Digital Abort')
                elif not value:
                    unabort(global_vars)
            
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
                    else:
                        telemetry.send(int(time.time() * 1000), elem.key, elem.nominal, data_tree, source, immediate=True)
                        to_actuate.append(elem)
                        
                    if elem.function_type == 'sequence' and element.control_type == 'switch':
                        element.func(*func_args)
                    elif elem.function_type == 'thread':
                        elem.stop_event.set()
                        if elem.thread != None:
                            elem.thread.join()
                            threads.remove((elem.name, elem.thread))
                            elem.thread = None
                
                
            # Triggers and Sequences
            if element.function_type == "sequence" and element.control_type == 'switch' and pressed and not inhibited and valid_source:
                element.func(*func_args)
            elif element.function_type == 'thread':
                if value:
                    element.stop_event.clear()
                    if element.thread == None:
                        element.thread = threading.Thread(target=element.func, args=func_args, daemon=True)
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
                if source == 'peripheral' and element.parent != elem.parent: # if a peripheral calls an actuation, midgard will ignore the elements in to_actuate and rely on the peripheral to make those actuations with midgard needing to instruct them
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

def abort(global_vars, cause='', verbose_cause=True, verbose_abort=True): # A digital abort exists, but cant be activated without an 'abort' switch (except during shutdown)
    debug_mode, show_server_logs, simulated_data, log_data, log_actuations, print_switch_changes, keep_csv, openmct_dir, log_output_dir, confirmation_keys, peripherals, threads, servers, stop_event, abort_state, non_abort_shutdown, startup_event, sync_groups, data_tree, all_keys, openmct, telemetry = global_vars
    
    if not abort_state.is_set():
        try: 
            abort_state.set() # sets first to stop anything else from writing and interfering with abort
            
            ###
            # Peripheral Abort Logic
            ###
            for peripheral in peripherals.values():
                if peripheral.manufacturer == 'NI':
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

def unabort(global_vars, verbose=True): 
    debug_mode, show_server_logs, simulated_data, log_data, log_actuations, print_switch_changes, keep_csv, openmct_dir, log_output_dir, confirmation_keys, peripherals, threads, servers, stop_event, abort_state, non_abort_shutdown, startup_event, sync_groups, data_tree, all_keys, openmct, telemetry = global_vars
    
    if abort_state.is_set():
        if verbose:
            print("\nUnaborted\n")
        abort_state.clear() # This just unsets the abort_state event

def shutdown(global_vars, do_abort=True):
    debug_mode, show_server_logs, simulated_data, log_data, log_actuations, print_switch_changes, keep_csv, openmct_dir, log_output_dir, confirmation_keys, peripherals, threads, servers, stop_event, abort_state, non_abort_shutdown, startup_event, sync_groups, data_tree, all_keys, openmct, telemetry = global_vars
    
    print("\n\n\nStopping MIDGARD...")
    stop_event.set() # Runs this to stop everything else from running and to enable the shutdown process
    try:
        if do_abort: # this is the propper shutdown
            try: # Ensure all relays are turned off on exit
                abort(global_vars, 'Shut Down', False, False)
            except Exception as e:
                _, _, tb = sys.exc_info()
                print(f'Error Closing Valves: {type(e).__name__} on line {tb.tb_lineno}: {e}')
            time.sleep(.1) # Short wait for NI, caused error when trying to close last ssr task without delay
        else:
            abort_state.set() # This is for special error shutdowns such as the synna cluster stopping while this program is running
        
        safe = True
        unsafe = []
        
        # Close peripherals
        write_actuation(global_vars, peripherals['Valhala_I'].elements['flight_phase'].key, 'shutdown', "auto", do_print=False)
        
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

