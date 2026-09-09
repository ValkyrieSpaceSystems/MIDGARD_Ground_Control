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

class global_vars():
    def __init__(self):
        self.log_data: bool = True
        self.log_actuations: bool = True
        self.simulated_data: bool = False
        self.keep_db: bool = True
        self.keep_logs: bool = True
        self.log_output_dir: str = ''
        self.print_actuations: bool = True
        self.show_server_logs: bool = False
        self.openmct_port: int = 4000
        self.telemetry_port: int = 4001
        self.confirmation_keys: list[str] = []
        self.debug_mode: bool = False
        self.health_check_interval = 2
        self.openmct_dir: str = ''
        
        self.peripherals = {}
        
        self.threads = []
        self.servers = []
        self.stop_event = threading.Event()
        self.abort_state = threading.Event()
        self.non_abort_shutdown = threading.Event()
        self.startup_event = threading.Event()
        
        self.sync_groups: dict[str, list] = {}
        self.data_tree = {}
        self.all_keys = []
        
        self.openmct = None
        self.telemetry = None
        
gv = global_vars()

# Helper Functions (Basic)
def print_out(msg):
    try:
        print(msg)
        if gv.telemetry is not None:
            if gv.telemetry.is_running:
                gv.telemetry.send(int(time.time() * 1000), 'terminal_log', msg, None, source="sequence")
    except Exception as e:
        _, _, tb = sys.exc_info()
        raise type(e)(f"[PrintOut] Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")

def get_element(data_tree, key, default=None):
    current = data_tree
    for key_part in key.split('.'):
        if isinstance(current, dict) and key_part in current:
            current = current[key_part]
        else:
            return default
    #print_out(current)
    return current

def combine_with_and(items, use_or=False, oxford_comma=True):
    items = list(items)
    
    if use_or:
        andor = 'or'
    else:
        andor = 'and'

    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} {andor} {items[1]}"

    *head, last = items
    sep = "," if oxford_comma else ""
    return f"{', '.join(head)}{sep} {andor} {last}"
    
def label(raw):
    return raw.replace('_', ' ').title()

def run(command, cwd=None, check=True, shell=False): 
    """Run a command and stop if it fails."""
    print_out(f"\n> {' '.join(command)}")
    subprocess.run(command, cwd=cwd, check=check, shell=shell)


# Operating Functions (Complex)
def check_configs(): # check that all config variables are the correct format with acceptable values
    try:
        issues = []
        warnings = []
        
        convars = {
            'log_data': {'var': gv.log_data, 'type': [bool], 'warning':{'val': [False], 'msg': 'Data not logged to file.'},},
            'log_actuations': {'var': gv.log_actuations, 'type': [bool], 'warning':{'val': [False], 'msg': 'Actuations not logged to file.'},},
            'simulated_data': {'var': gv.simulated_data, 'type': [bool], 'warning':{'val': [True], 'msg': 'Simulated data in use. Real data ignored.'},},
            'keep_db': {'var': gv.keep_db, 'type': [bool], 'warning':{'val': [False], 'msg': 'Telemetry database removed after run. No data backup after closing.'},},
            'keep_logs': {'var': gv.keep_logs, 'type': [bool], 'warning':{'val': [False], 'msg': 'Log file removed after run. No log data after closing.'},},
            'log_output_dir': {'var': gv.log_output_dir, 'type': [str], 'subtype': 'dir',},
            'print_actuations': {'var': gv.print_actuations, 'type': [bool], 'warning':{'val': [False], 'msg': 'Actuations not output to terminal.'},},
            'show_server_logs': {'var': gv.show_server_logs, 'type': [bool],},
            'openmct_port': {'var': gv.openmct_port, 'type': [int], 'subtype': 'port',},
            'telemetry_port': {'var': gv.telemetry_port, 'type': [int], 'subtype': 'port',},
            'confirmation_keys': {'var': gv.confirmation_keys, 'type': [list], 'subtype': [str]},
            'debug_mode': {'var': gv.debug_mode, 'type': [bool], 'warning':{'val': [True], 'msg': 'Debug Mode enabled. Normally restricted operations are now possible'},},
            'health_check_interval': {'var': gv.health_check_interval, 'type': [int,float],},
            'openmct_dir': {'var': gv.openmct_dir, 'type': [str], 'subtype': 'dir',},
        }
        
        for cv, cvars in convars.items():
            # Config Variable Types
            if type(cvars['var']) not in cvars['type']: 
                issues.append(f"TypeError: {cv} variable '{cvars['var']}' is a {type(cvars['var'])}. Must be {combine_with_and([t.__name__ for t in cvars['type']],use_or=True)}")
            if 'subtype' in cvars:
                if type(cvars['var']) == list:
                    for sub in cvars['var']:
                        if type(sub) not in cvars['subtype']: 
                            issues.append(f"TypeError: {cv} variable content '{sub}' is a {type(sub)}. Must be {combine_with_and([t.__name__ for t in cvars['subtype']],use_or=True)}")
                        
            # Config Variable Values
            if 'subtype' in cvars:
                if cvars['subtype'] == 'dir': # check directory exists or make it and is writeable
                    if os.path.isdir(os.path.abspath(cvars['var'])): # if it already exists
                        pass
                    else:
                        parent = os.path.abspath(cvars['var'])
                        while not os.path.exists(parent):
                            parent = os.path.dirname(parent)
                        if os.path.isdir(parent) and os.access(parent, os.W_OK): # checks if the parent is writeable
                            os.makedirs(os.path.abspath(cvars['var']), exist_ok=True)
                        else:
                            issues.append(f"Cannot create directory: {os.path.abspath(cvars['var'])}")
                elif cvars['subtype'] == 'port': # check port is open
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                        try:
                            sock.bind(("127.0.0.1",cvars['var']))
                        except OSError:
                            issues.append(f"OSError: {cv} variable '{cvars['var']}' port already in use")
            if 'warning' in cvars:
                if cvars['var'] in cvars['warning']['val']:
                    warnings.append(f"{cv} variable value is {cvars['var']}. {cvars['warning']['msg']}")

        # Raise issues
        if warnings != []:
            print_out(f'\nWARNING, WARNING, WARNING\n\n{"\n\n".join(warnings)}\n\nWARNING, WARNING, WARNING\n\n')
        if issues != []:
            raise Exception("\n"+"\n\n".join(issues))
        if issues == [] and warnings == []:
            print_out(f'Config variable check passed successfully')
            
    except Exception as e:
        _, _, tb = sys.exc_info()
        raise type(e)(f"[CheckConfig] Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
    
def check_and_install_openmct(): # check that openmct is installedp properly and install it if it isnt installed already
    # Check if there is an OpenMCT directory
    if os.listdir(gv.openmct_dir): # Check if openmct folder has items inside
        try:
            package_path = os.path.join(gv.openmct_dir, "package.json") 
            if not os.path.isfile(package_path): # Check if package.json existst
                raise FileNotFoundError(f"OpenMCT Directory is not not complete: package.json does not exist: {package_path}")

            # Read package.json
            with open(package_path, "r", encoding="utf-8") as file:
                package = json.load(file)
                # Check if the package name is "openmct"
                if package.get("name") != "openmct": 
                    raise ValueError(f'OpenMCT Directory is not OpenMCT: {package.get("name")!r}')
                installed_version = f'v{package.get("version")}'
                
                headers = {
                    "Accept": "application/vnd.github+json",
                    "User-Agent": "latest-github-version-script",
                }
                token = os.environ.get("GITHUB_TOKEN")
                if token:
                    headers["Authorization"] = f"Bearer {token}"
            
                req = urllib.request.Request(f"https://api.github.com/repos/nasa/openmct/releases/latest", headers=headers)
                with urllib.request.urlopen(req) as response:
                    latest_version = json.loads(response.read().decode()).get("tag_name")
                    if latest_version != installed_version:
                        print_out(installed_version)
                        print_out(f"[InstallOpenMCT] new version of OpenMCT available [{latest_version}]. To install, delete openmct folder and restart")
        
        except urllib.error.URLError:
            pass
        except Exception as e:
            _, _, tb = sys.exc_info()
            raise type(e)(f"[InstallOpenMCT] Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")

    else:
        # Install OpenMCT
        try:
            if shutil.which('git') is None:
                raise RuntimeError("Git is not installed or is not available in PATH.")
            if shutil.which("node") is None:
                raise RuntimeError("Node.js is not installed. Install Node.js 24+ from https://nodejs.org/en/download and run this again.")
            if shutil.which("npm") is None:
                raise RuntimeError("npm is not installed or is not available in PATH.")
            
            print_out(f"[Install OpenMCT] OpenMCT directory not found. Installing OpenMCT at {str(gv.openmct_dir)}")
            
            run(["git", "clone", "https://github.com/nasa/openmct.git", str(gv.openmct_dir)])
            run(["npm", "install"], cwd=gv.openmct_dir)
            run(["npm", "audit", "fix"], cwd=gv.openmct_dir, check=False)
            run(["npm", "run", "build"], cwd=gv.openmct_dir)
            
            print_out(f"[InstallOpenMCT] OpenMCT directory not found. OpenMCT installed at {str(gv.openmct_dir)}")
            
        except Exception as e:
            _, _, tb = sys.exc_info()
            shutil.rmtree(gv.openmct_dir)
            print_out(f"[Install OpenMCT] Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")

def check_peripherals(): # check that all peripherals are configured properly
    try:
        issues = []
        warnings = []
        
    except Exception as e:
        _, _, tb = sys.exc_info()
        raise type(e)(f"[CheckPeripherals] Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")

def write_actuation(key, requested, source, missing_keys=None, do_print=True, bypass_checks=False):
    element = get_element(gv.data_tree, key)
    
    try:
        if not gv.stop_event.is_set():
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
                source_authorized = True if source in requested_state.sources or requested_state.sources == [] else False
                transition_allowed = True if requested_state in current_state.transition_to else False
                interlocked = any(not bool(elem.value) for elem in requested_state.armed_by) or any(bool(elem.value) for elem in requested_state.disarmed_by)
            else:
                transition_allowed = True
                source_authorized = True if source in element.sources or element.sources == [] else False
                interlocked = any(not bool(elem.value) for elem in element.armed_by) or any(bool(elem.value) for elem in element.disarmed_by)
            
            if source == 'user' and missing_keys != []:
                confirmation_keys_pressed = False
            else:
                confirmation_keys_pressed = True
                
            if bypass_checks:
                source_authorized = True
                transition_allowed = True
                interlocked = False
                confirmation_keys_pressed = True
            
            run_sequence = False
            if not confirmation_keys_pressed or not source_authorized or not transition_allowed: # Stay the same
                value = element.value
            elif interlocked: # Nominalize
                value = element.nominal
            else: # Actuate
                value = requested
                run_sequence = True
                
                
            # Sync Groups
            if element.sync_group == 'abort':
                if value:
                    abort('Digital Abort')
                elif not value:
                    unabort()
            
            elif element.sync_group == 'logging':
                if value:
                    gv.telemetry.start_logging()
                elif not value:
                    gv.telemetry.stop_logging()
                
            if element.sync_group:
                for elem in gv.sync_groups.get(element.sync_group, []):
                    if elem is not element:
                        gv.telemetry.send(int(time.time() * 1000), elem.key, value, element, source)
                        to_actuate.append(elem)
                    
                    
            # Inhibiting
            nominalize_array = []
            uninhibit_array = []
            if element.control_type == "selector":
                nominalize_array = element.states_index[value].disarms + element.states_index[element.value].arms 
                uninhibit_array = element.states_index[element.value].disarms + element.states_index[value].arms 
            elif element.control_type == "switch":
                if value:
                    nominalize_array = element.disarms
                    uninhibit_array = element.arms
                else:
                    nominalize_array = element.arms
                    uninhibit_array = element.disarms
                    
                    
            # Write to GUI
            gv.telemetry.send(int(time.time() * 1000), element.key, value, element, source, inhibited=interlocked or not transition_allowed,immediate=True) # Need to update the element first so other elements can check the element's new value
            to_actuate.append(element)
                
            to_nominalize = []
            if nominalize_array:
                prop_complete = False
                while not prop_complete: # this populates the nominalize_array array with all the elements to be nominalized by the nominalizing of all the elements already in nominalize_array
                    start = nominalize_array
                    for e in nominalize_array:
                        nominalize_array = nominalize_array + [elem for elem in e.arms if elem not in nominalize_array and elem.value == True and elem.control_type != 'button']
                    if nominalize_array == start: prop_complete = True 
                
                to_nominalize = [elem for elem in nominalize_array if elem.value == True and elem.element_class != 'State' and elem.control_type != 'button'] + [elem.parent for elem in nominalize_array if elem.element_class == 'State' and elem.parent.value != elem.parent.nominal]
                to_nominalize = list(dict.fromkeys(to_nominalize))
                
                for elem in nominalize_array: # Nominalize the elements
                    if elem.element_class == 'State': # Selector logic
                        par = elem.parent
                        if par.states_index[par.nominal] in par.states_index[par.value].transition_to: # Checks if transition is valid
                            gv.telemetry.send(int(time.time() * 1000), par.key, par.nominal, par, source, immediate=True)
                            
                            if hasattr(par, 'function_type'):
                                if par.states_index[par.nominal] in par.function_states or par.function_states == []: # nominal state not inhibited
                                    if par.function_type == 'sequence' and par.run_on_nominalize: 
                                        par.func(par)
                                    elif par.function_type == 'thread' and par.run_on_nominalize: 
                                        element.stop_event.clear()
                                        if element.thread == None:
                                            element.thread = threading.Thread(target=element.func, args=(element,), daemon=True)
                                            element.thread.start()
                                            gv.threads.append((element.name, element.thread))
                                else: 
                                    if par.function_type == 'thread':
                                        par.stop_event.set()
                                        if par.thread != None:
                                            par.thread.join()
                                            gv.threads.remove((par.name, par.thread))
                                            par.thread = None
                    else:
                        gv.telemetry.send(int(time.time() * 1000), elem.key, elem.nominal, elem, source, inhibited=True, immediate=True)
                        if elem.control_type in ['switch']: # only switches should always change on nominalization (selector logic above)
                            to_actuate.append(elem)
                            if hasattr(elem, 'function_type'):
                                if elem.function_type == 'sequence':
                                    elem.func(element)
                                elif elem.function_type == 'thread':
                                    elem.stop_event.set()
                                    if elem.thread != None:
                                        elem.thread.join()
                                        gv.threads.remove((elem.name, elem.thread))
                                        elem.thread = None
            
            if uninhibit_array: # Uninhibit the elements
                for elem in uninhibit_array:
                    if elem.element_class == "State":\
                        gv.telemetry.send(int(time.time() * 1000), elem.parent.key, elem.parent.value, elem.parent, source, immediate=True) # Just updating the selector because selector inhibit logic in telemetry.send
                    else:
                        elem_interlocked = any(not bool(e.value) for e in elem.armed_by) or any(bool(e.value) for e in elem.disarmed_by)
                        gv.telemetry.send(int(time.time() * 1000), elem.key, elem.nominal, elem, source, inhibited=elem_interlocked, immediate=True)
            
            
            # Triggers and Sequences
            if hasattr(element, 'function_type'):
                if (element.control_type == 'selector' and (element.states_index[value] in element.function_states or element.function_states == [])) or (element.control_type != 'selector' and value): # If selector with a valid state or no valid states (ie all states) or if not selector and value
                    if element.function_type == "sequence" and run_sequence: # regular sequences will run every actuation, sequence must check the element value
                        element.func(element)
                    elif element.function_type == 'thread':
                        element.stop_event.clear()
                        if element.thread == None:
                            element.thread = threading.Thread(target=element.func, args=(element,), daemon=True)
                            element.thread.start()
                            gv.threads.append((element.name, element.thread))
                else:
                    if element.function_type == 'thread':
                        element.stop_event.set()
                        if element.thread != None:
                            element.thread.join()
                            gv.threads.remove((element.name, element.thread))
                            element.thread = None
            
            
            # Physical Actuation
            write_ni = False
            ni_peripherals = []
            for elem in to_actuate:
                if source == 'peripheral' and element.parent != elem.parent: # if a peripheral calls an actuation, midgard will ignore the elements in to_actuate and rely on the peripheral to make those actuations with midgard needing to instruct them
                    if elem.type in ['valve','ssr','servo','pyro']: # if physical actuator
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
                    print_out(f'{element.name} now inhibiting and setting nominal {[elem.name for elem in to_nominalize]}')
                    
                if source_authorized and transition_allowed and confirmation_keys_pressed:
                    if element.control_type == "switch":
                        if element.value == False:
                            display_value = element.nominal_state
                        else:
                            display_value = element.off_nominal_state
                    elif element.control_type == "selector":
                        display_value = element.states_name[value]
                    else:
                        display_value = value
                        
                    if element.control_type == "button":
                        if interlocked: 
                            control_type_display = f"not triggered"
                        else:
                            control_type_display = f"triggered"
                    elif element.type == "sequence":
                        control_type_display = f"{display_value}"
                    else:
                        control_type_display = f"set to {display_value}"
                                            
                    if not interlocked:
                        print_out(f"{element.name} {control_type_display}")
                    elif interlocked:
                        if element.control_type == 'selector':
                            blocked = [elem.name for elem in requested_state.armed_by if elem.value == False] + [elem.name for elem in requested_state.disarmed_by if elem.value == True]  # A list of all inhbiiting elements
                        else:
                            blocked = [elem.name for elem in element.armed_by if elem.value == False] + [elem.name for elem in element.disarmed_by if elem.value == True]  # A list of all inhbiiting elements
                        print_out(f"{element.name} {control_type_display}. Interlocked by {blocked}")
                
                else: 
                    reasons = []
                    if not confirmation_keys_pressed: # Confirmation keys not pressed
                        missing_key_names = []
                        for k in missing_keys:
                            if isinstance(k, str):
                                missing_key_names.append(k.upper())
                            elif isinstance(k, pynput.keyboard.Key):
                                missing_key_names.append(k.name.replace('_', ' ').title())
                        
                        reasons.append(f"{missing_key_names} not pressed")
                    
                    if element.control_type == 'selector':
                        if not source_authorized:
                            reasons.append(f"{source} not valid source {requested_state.sources}")
                            
                        if not transition_allowed:
                            reasons.append(f"{requested_state.name} not valid transition {[s.name for s in current_state.transition_to if s.name != current_state.name ]}")
                    else:
                        if not source_authorized:
                            reasons.append(f"{source} not valid source {element.sources}")
                        
                    print_out(f"{element.name} command ignored because {combine_with_and(reasons)}")

                    
    except Exception as e:
        _, _, tb = sys.exc_info()
        print_out(f"{element.name} Write Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")

def abort(cause='', verbose_cause=True, verbose_abort=True): # A digital abort exists, but cant be activated without an 'abort' switch (except during shutdown)
    if not gv.abort_state.is_set():
        try: 
            gv.abort_state.set() # sets first to stop anything else from writing and interfering with abort
            
            ###
            # Peripheral Abort Logic
            ###
            for peripheral in gv.peripherals.values():
                if peripheral.manufacturer == 'NI':
                    for writer in peripheral.writers: # Iterates through all writers and sets all channels on each module to false
                        writer.write_one_sample_one_line(np.array([False]*8))
                    
                elif peripheral.manufacturer == 'LabJack':
                    ljm.eWriteNames(peripheral.handle, len(peripheral.labjack_actuators), [elem.channel for elem in peripheral.labjack_actuators], [0]*len(peripheral.labjack_actuators))
                    
            if verbose_abort: 
                print_out("\nABORTING\nABORTING\nABORTING\n")
            if cause != '' and verbose_cause:
                print_out(f'Abort: {cause}\n')
        except Exception as e:
            _, _, tb = sys.exc_info()
            print_out(f"Abort Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")

def unabort(verbose=True): 
    if gv.abort_state.is_set():
        if verbose:
            print_out("\nUnaborted\n")
        gv.abort_state.clear() # This just unsets the abort_state event

def shutdown(do_abort=True):
    print_out("\n\n\nStopping MIDGARD...")
    gv.stop_event.set() # Runs this to stop everything else from running and to enable the shutdown process
    try:
        if do_abort: # this is the propper shutdown
            try: # Ensure all relays are turned off on exit
                abort('Shut Down', False, False)
            except Exception as e:
                _, _, tb = sys.exc_info()
                print_out(f'Error Closing Valves: {type(e).__name__} on line {tb.tb_lineno}: {e}')
            time.sleep(.1) # Short wait for NI, caused error when trying to close last ssr task without delay
        else:
            gv.abort_state.set() # This is for special error shutdowns such as the synna cluster stopping while this program is running
        
        safe = True
        unsafe = []
        
        # Close peripherals
        write_actuation(gv.peripherals['Valhala_I'].elements['flight_phase'].key, 'shutdown', "auto", do_print=False)
        
        for peripheral in gv.peripherals.values():
            if peripheral.manufacturer == 'VSS':
                try: 
                    pass
                except Exception as e:
                    _, _, tb = sys.exc_info()
                    print_out(f'[Shutdown] Error: {peripheral.display_name}: {type(e).__name__} on line {tb.tb_lineno}: {e}')
            
            elif peripheral.manufacturer == 'NI':
                closed_tasks = []
                for module in peripheral.ni_modules.values(): # Closes all the NI tasks
                    try:
                        module.task.close()
                        closed_tasks.append(f'{module.name} ({module.module_number})')
                    except Exception as e:
                        _, _, tb = sys.exc_info()
                        print_out(f'[Shutdown] Error: {peripheral.display_name} NI Task for module {module.name} ({module.module_number}): {type(e).__name__} on line {tb.tb_lineno}: {e}')
                print_out(f'[Shutdown] Closed NI tasks for {combine_with_and(closed_tasks)}')
                
            elif peripheral.manufacturer == 'LabJack':
                try: # closes the labjack handle
                    ljm.close(peripheral.handle)
                except Exception as e:
                    _, _, tb = sys.exc_info()
                    print_out(f'[Shutdown] Error: {peripheral.display_name} LabJack did not close properly: {type(e).__name__} on line {tb.tb_lineno}: {e}')
                    
        
        for name, server in gv.servers: # stops all running threads (runs after closing ni tasks because threads call ni tasks while running, would cause an error if reversed order)
            server.stop(delete_db=not gv.keep_db)
            if server.is_running:
                print_out(f"[Shutdown] Warning: {name} server did not stop cleanly")
                safe = False
                unsafe.append(name)
                
        for name, thread in gv.threads: # stops all running threads (runs after closing ni tasks because threads call ni tasks while running, would cause an error if reversed order)
            thread.join(timeout=5)
            if thread.is_alive():
                print_out(f"[Shutdown] Warning: {name} server did not stop cleanly")
                safe = False
                unsafe.append(name)
                
        if not gv.keep_logs: # Removes csv logs if that setting is set
            print_out('[Shutdown] removing logs')
            for file in gv.telemetry.csv_files:
                try:
                    os.remove(file)
                except FileNotFoundError:
                    pass
                except Exception as e:
                    print_out(f'Failed to remove file {file}: {type(e).__name__} on line {tb.tb_lineno}: {e}')
                
                
    except Exception as e:
        _, _, tb = sys.exc_info()
        print_out(f"[Shutdown] Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
            
    if safe: print_out("\n\n[Shutdown] All systems stopped safely\n\n")
    else: print_out(f"\n\n[Shutdown] Threads or servers did not stop safely: {unsafe}\n\n")

