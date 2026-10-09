import time, csv, os, threading, sys, shutil, re, tomllib, math, pynput, csv, tomllib, importlib.util, asyncio, json, sqlite3, uvicorn, subprocess, urllib.request, urllib.error, socket, platform, serial, shlex
import numpy as np
from queue import PriorityQueue, Queue, Empty, Full
from datetime import datetime, UTC
from pymavlink import mavutil

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

_G = 9.80665  # standard gravity, m/s^2

MIN_NODE_VERSION: list[int] | None = [24, 14, 1]
NODE_DOWNLOAD_URL = "https://nodejs.org/en/download"
NVM_WINDOWS_URL = "https://github.com/nvm-windows/nvm/releases/latest"

# unit -> (kind, scale, offset), where value_in_base_unit = value * scale + offset.
# Only units of the same kind can be converted to each other.
UNITS = {
    # angle (base: rad)
    "rad": ("angle", 1.0, 0.0),
    "deg": ("angle", math.pi / 180, 0.0),
    "cdeg": ("angle", math.pi / 18000, 0.0),
    "degE7": ("angle", math.pi / 1.8e9, 0.0),
    # angular rate (base: rad/s)
    "rad/s": ("angular_rate", 1.0, 0.0),
    "mrad/s": ("angular_rate", 1e-3, 0.0),
    "deg/s": ("angular_rate", math.pi / 180, 0.0),
    # length (base: m)
    "m": ("length", 1.0, 0.0),
    "mm": ("length", 1e-3, 0.0),
    "cm": ("length", 1e-2, 0.0),
    "dm": ("length", 0.1, 0.0),
    "dam": ("length", 10.0, 0.0),
    "km": ("length", 1000.0, 0.0),
    "in": ("length", 0.0254, 0.0),
    "ft": ("length", 0.3048, 0.0),
    "mi": ("length", 1609.344, 0.0),
    "nmi": ("length", 1852.0, 0.0),
    # speed (base: m/s)
    "m/s": ("speed", 1.0, 0.0),
    "cm/s": ("speed", 1e-2, 0.0),
    "dm/s": ("speed", 0.1, 0.0),
    "km/h": ("speed", 1 / 3.6, 0.0),
    "mph": ("speed", 0.44704, 0.0),
    "knots": ("speed", 1852 / 3600, 0.0),
    "ft/s": ("speed", 0.3048, 0.0),
    # acceleration (base: m/s/s); "mG" is milli-g
    "m/s/s": ("acceleration", 1.0, 0.0),
    "m/s^2": ("acceleration", 1.0, 0.0),
    "mG": ("acceleration", _G / 1000, 0.0),
    "g": ("acceleration", _G, 0.0),
    # pressure (base: Pa)
    "Pa": ("pressure", 1.0, 0.0),
    "hPa": ("pressure", 100.0, 0.0),
    "mbar": ("pressure", 100.0, 0.0),
    "kPa": ("pressure", 1000.0, 0.0),
    # time (base: s)
    "s": ("time", 1.0, 0.0),
    "ds": ("time", 0.1, 0.0),
    "ms": ("time", 1e-3, 0.0),
    "us": ("time", 1e-6, 0.0),
    # electrical
    "V": ("voltage", 1.0, 0.0),
    "mV": ("voltage", 1e-3, 0.0),
    "A": ("current", 1.0, 0.0),
    "cA": ("current", 1e-2, 0.0),
    "mA": ("current", 1e-3, 0.0),
    # magnetic field (base: gauss)
    "gauss": ("magnetic_field", 1.0, 0.0),
    "mgauss": ("magnetic_field", 1e-3, 0.0),
    # percent (base: %)
    "%": ("percent", 1.0, 0.0),
    "d%": ("percent", 0.1, 0.0),
    "c%": ("percent", 0.01, 0.0),
    # temperature (base: degC)
    "degC": ("temperature", 1.0, 0.0),
    "cdegC": ("temperature", 0.01, 0.0),
    "K": ("temperature", 1.0, -273.15),
    "degF": ("temperature", 5 / 9, -32 * 5 / 9),
}

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

def convert_units(element, value, from_unit, to_unit):
    """
    Convert `value` from `from_unit` to `to_unit`.
 
    `value` can be a number, a list/tuple of numbers (converted element by
    element), or anything that supports arithmetic (e.g. a NumPy array).
    Raises ValueError for an unknown unit or for units of different kinds
    (e.g. "rad" -> "m").
    """
    try:
        if from_unit == to_unit or from_unit == None:
            return value
    
        try:
            kind_from, scale_from, off_from = UNITS[from_unit]
            kind_to, scale_to, off_to = UNITS[to_unit]
        except KeyError as e:
            raise ValueError(f"Unknown unit {e.args[0]!r}") from None
        if kind_from != kind_to:
            raise ValueError(f"Cannot convert {from_unit!r} ({kind_from}) to {to_unit!r} ({kind_to})")
    
        def one(v):
            return ((v * scale_from + off_from) - off_to) / scale_to
    
        if isinstance(value, (list, tuple)):
            return [one(v) for v in value]
        return one(value)
    except Exception as e:
        _, _, tb = sys.exc_info()
        print_out(f"[{element.name}] Convert Units Error for value={value}, from_unit={from_unit}, to_unit={to_unit}: {type(e).__name__} on line {tb.tb_lineno}: {e}") 


# Operating Functions (Complex)
def check_configs(): # check that all config variables are the correct format with acceptable values
    try:
        print()
        
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
                    if platform.system() == "Windows":
                        result = subprocess.run(
                            ["netstat", "-ano"],
                            capture_output=True,
                            text=True
                        )

                        for line in result.stdout.splitlines():
                            parts = line.split()

                            if len(parts) >= 4:
                                local_address = parts[1]
                                state = parts[3]

                                if local_address.endswith(f":{cvars['var']}") and state == "LISTENING":
                                    issues.append(f"OSError: {cv} variable '{cvars['var']}' port already in use")
                    
                    else:
                        result = subprocess.run(
                            ["lsof", "-i", f":{cvars['var']}"],
                            capture_output=True,
                            text=True
                        )
                        for line in result.stdout.splitlines()[1:]:
                            if "LISTEN" in line:
                                issues.append(f"OSError: {cv} variable '{cvars['var']}' port already in use")
            
            if 'warning' in cvars:
                if cvars['var'] in cvars['warning']['val']:
                    warnings.append(f"{cv} variable value is {cvars['var']}. {cvars['warning']['msg']}")
                    
        # Check for prerequisite software
        def get_version(command: str) -> str | None:
            """Return the output of `<command> --version`, or None if it isn't usable."""
            # shutil.which respects PATHEXT on Windows, so "npm" resolves to npm.cmd
            path = shutil.which(command)
            macos_git = False
            if command == 'git':
                if platform.system() != "Darwin" or path != "/usr/bin/git":
                    macos_git = False
                try:
                    macos_git = subprocess.run(["xcode-select", "-p"], capture_output=True).returncode != 0
                except OSError:
                    macos_git = False
            
            if path is None or macos_git:
                return None
            try:
                result = subprocess.run(
                    [path, "--version"], capture_output=True, text=True, timeout=15
                )
            except (OSError, subprocess.SubprocessError):
                return None
            if result.returncode != 0:
                return None
            return result.stdout.strip() or None
        
        system = platform.system()  # 'Windows', 'Darwin' or 'Linux'
        
        # git
        git_version = get_version("git")
        if git_version:
            print(f"[Git] Git installed with version {git_version}")
        else:
            if system == "Windows":
                issues.append(f"SystemError: Git not installed. Download and run the installer: https://git-scm.com/download/win")

            elif system == "Darwin":
                issues.append(f"SystemError: Git not installed. Run in Terminal: 'xcode-select --install'")
            
            elif system == "Linux":
                # Linux: suggest the command for whichever package manager is present
                LINUX_PACKAGE_MANAGERS = [
                    ("apt-get", ["apt-get", "install", "-y"]),
                    ("dnf", ["dnf", "install", "-y"]),
                    ("yum", ["yum", "install", "-y"]),
                    ("pacman", ["pacman", "-S", "--noconfirm"]),
                    ("zypper", ["zypper", "--non-interactive", "install"]),
                    ("apk", ["apk", "add"]),
                ]
                for executable, command in LINUX_PACKAGE_MANAGERS:
                    if shutil.which(executable):
                        issues.append(f"SystemError: Git not installed. Run: 'sudo {shlex.join(command + ['git'])}'")
                issues.append(f"SystemError: Git not installed. Install git using your package manager: https://git-scm.com/download/linux")
    
        # node
        node_version = get_version("node")
        if system == "Windows":
            node_install  = f"Install nvm-windows (latest release): {NVM_WINDOWS_URL}. Afterwards, open Powershell as administrator and run 'nvm install lts && nvm use lts'. After installing, close and reopen your terminal, then run this program again."
        else:
            node_install = f"Download and install Node.js (npm is included): {NODE_DOWNLOAD_URL}. After installing, close and reopen your terminal, then run this program again."
        
        if node_version is None:
            issues.append(f"SystemError: Node.js not installed. {node_install}")
        else:
            node_version = [int(ver) for ver in node_version.lstrip("v").split(".")]
            if MIN_NODE_VERSION and node_version is not None and node_version >= MIN_NODE_VERSION:
                issues.append(f"SystemError: Node.js is too old. Version {".".join(map(str,MIN_NODE_VERSION))} or newer is required. Reinstall Node.js to install a valid version. {node_install}")
            elif node_version is not None:
                print(f"[Node.js] Node.js installed with version {node_version}")
    
            # npm
            npm_version = get_version("npm")
            if npm_version:
                print(f"[NPM] NPM installed with version {npm_version}")
            else:
                issues.append(f"SystemError: NPM is not installed. Reinstall Node.js to install NPM. {node_install}")

        # Raise issues
        print()
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
                try:
                    with urllib.request.urlopen(req) as response:
                        latest_version = json.loads(response.read().decode()).get("tag_name")
                        if latest_version != installed_version:
                            print_out(installed_version)
                            print_out(f"[InstallOpenMCT] new version of OpenMCT available [{latest_version}]. To install, delete openmct folder and restart")
                except OSError: 
                    raise OSError(f"[InstallOpenMCT] No interenet access to download OpenMCT. {type(e).__name__} on line {tb.tb_lineno}: {e}")
        
        except urllib.error.URLError:
            pass
        except Exception as e:
            _, _, tb = sys.exc_info()
            raise type(e)(f"[InstallOpenMCT] Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")

    else:
        # Install OpenMCT
        try:
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

def write_actuation(key, requested, source, missing_keys=None, do_print=gv.print_actuations, bypass_checks=False):
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
                if type(peripheral) != dict:
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
        # Do something here to transmit shutdown to peripherals
        
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


# Workers
def peripheral_worker(peripheral):
    try:
        def handle_data(data):
            try:
                max_utc = max(data)
                for ts, sample in sorted(data.items()):
                    for key, value in sample.items():
                        element = get_element(gv.data_tree, key)
                        if element.element_class in ['Actuator', 'State']:
                            write_actuation(key, value, "peripheral")
                        elif ts == max_utc:
                            print_to_gui = True if ts - element.last_ts > 95 else False # Refresh at ~10Hz or less
                            gv.telemetry.send(ts, key, value, element, "peripheral", push_to_gui=print_to_gui)
                            if print_to_gui:
                                element.last_ts = ts
                        else:
                            gv.telemetry.send(ts, key, value, element, "peripheral", push_to_gui=False)
            except Exception as e:
                _, _, tb = sys.exc_info()
                print_out(f"[{peripheral.worker_name}] Data Handling Error: {type(e).__name__} on line {tb.tb_lineno}: {e}") 
                        
        if gv.simulated_data:
            i = 0
            j = False
            str_list = ['a','b','c','d','e','f','g','h','i','j','k']
            peripheral.init_event.set()
            print_out(f"[{peripheral.worker_name}] started")
            while not gv.stop_event.is_set():
                data = {}
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
                
                handle_data(data)
        
        elif peripheral.manufacturer == 'NI':
            peripheral.init_event.set()
            print_out(f"[{peripheral.worker_name}] started")
            while not gv.stop_event.is_set():
                try:
                    data = peripheral.data_queue.get(timeout=0.2)
                    handle_data(data)
                except Empty:
                    continue
                except Exception as e:
                    _, _, tb = sys.exc_info()
                    print_out(f"[{peripheral.worker_name}] NI Read Error: {type(e).__name__} on line {tb.tb_lineno}: {e}") 
                    
        elif peripheral.manufacturer == 'LabJack':
            peripheral.init_event.set()
            print_out(f"[{peripheral.worker_name}] started")
            while not gv.stop_event.is_set():
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
                    
                    data = {}
                    for i, sample in enumerate(sensor_data): # Number of data samples
                        ts = ts_end - (len(sensor_data) - i) * (1/peripheral.pull_freq) * 1e9  # the timestamp we get is from the last data point so we need to calculate the timestamps backwards from this
                        data[ts] = {}
                        for j, channel in enumerate(peripheral.channels): # Number of channels
                            data[ts][channel.key] = sample[j] * channel.scale + channel.offset
                    handle_data(data)
                    
                except Exception as e:
                    _, _, tb = sys.exc_info()
                    print_out(f"[{peripheral.worker_name}] LabJack Read Error: {type(e).__name__} on line {tb.tb_lineno}: {e}") 
                    
        elif peripheral.interface.protocol == 'mavlink':
            while not gv.stop_event.is_set():
                try:
                    if peripheral.interface.connection == None:
                        peripheral.interface.connect()
                        
                        mavlink_lookup = {}
                        for element in peripheral.elements.values(): # connect elements to a mavlink message and field name
                            if hasattr(element,'message_type') and hasattr(element,'field_name'):
                                if element.message_type not in [None, ''] and element.field_name not in [None, '']:
                                    mavlink_lookup.setdefault(element.message_type.upper(), {}).setdefault(element.field_name, []).append(element)
                        id_and_hz = []
                        for message_type in mavlink_lookup: # find the largest sample rate per message
                            hz = 0
                            for field_name in mavlink_lookup[message_type]:
                                for element in mavlink_lookup[message_type][field_name]:
                                    if element.sample_rate_hz > hz:
                                        hz = element.sample_rate_hz
                            id_and_hz.append((message_type,hz))
                        for name, hz in id_and_hz: # request message stream
                            msg_id = getattr(mavutil.mavlink, f"MAVLINK_MSG_ID_{name}")
                            peripheral.interface.connection.mav.command_long_send(
                                peripheral.interface.connection.target_system,
                                peripheral.interface.connection.target_component,
                                mavutil.mavlink.MAV_CMD_SET_MESSAGE_INTERVAL,
                                0,                  # confirmation
                                msg_id,             # param1: message ID
                                int(1e6 / hz),      # param2: interval in microseconds
                                0, 0, 0, 0, 0,      # params 3-7 unused
                            )
                        
                        if not peripheral.init_event.is_set():
                            print_out(f"[{peripheral.worker_name}] started with connection on {peripheral.device}")
                        peripheral.init_event.set()
                        
                        
                        last_ts = time.time()
                        while not gv.stop_event.is_set():
                            try:
                                msg = peripheral.interface.connection.recv_match(blocking=True, timeout=1)
                                ts = int(time.time() * 1000)
                                if msg is None:
                                    if time.time() - last_ts > peripheral.interface.timeout:
                                        print(f"[{peripheral.worker_name}] Connection Error: No data received, link lost. Retrying")
                                        break
                                    continue
                                if msg.get_type() == "BAD_DATA":
                                    continue
                                last_ts = time.time()
                                
                                data = {ts:{}}
                                msg_dict = msg.to_dict()
                                for field_name, elements in mavlink_lookup.get(msg_dict.get("mavpackettype"), {}).items():
                                    if field_name in msg_dict:
                                        for element in elements:
                                            value = convert_units(element, msg_dict[field_name],msg.fieldunits_by_name.get(field_name),element.unit) # value, default_unit, desired_unit
                                            data[ts][element.key] = value
                                handle_data(data)
                                
                            except Exception as e:
                                _, _, tb = sys.exc_info()
                                print_out(f"[{peripheral.worker_name}] MAVLink Read Error: {type(e).__name__} on line {tb.tb_lineno}: {e}") 
                                if type(e).__name__ == 'SerialException': 
                                    break
                                
                except Exception as e:
                    _, _, tb = sys.exc_info()
                    print_out(f"[{peripheral.worker_name}] MAVLink Connection Error: {type(e).__name__} on line {tb.tb_lineno}: {e}") 
                finally:
                    peripheral.interface.connection.close()
            
        elif peripheral.interface.protocol == 'elrs':
            peripheral.init_event.set()
            print_out(f"[{peripheral.worker_name}] started")
            while not gv.stop_event.is_set():
                try:
                    data = {}
                    time.sleep(0.2)
                    
                except Exception as e:
                    _, _, tb = sys.exc_info()
                    print_out(f"[{peripheral.worker_name}] ELRS Read Error: {type(e).__name__} on line {tb.tb_lineno}: {e}") 
                
    except Exception as e:
        if not gv.stop_event.is_set():
            _, _, tb = sys.exc_info()
            print_out(f"[{peripheral.worker_name}] Setup Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
    finally: 
        print_out(f"[{peripheral.worker_name}] stopped")

def gui_worker(init_event):
    # Handles all user inputs from GUI
    try:
        for key in gv.all_keys: # Populate default values
            element = get_element(gv.data_tree, key)
            default = getattr(element, 'default', None)
            if default is not None:
                write_actuation(key, default, "auto", do_print=False)

        init_event.set()
        print_out(f"[GUI Worker] started")
        while not gv.stop_event.is_set():
            try:
                cmd = gv.telemetry.command_queue.get(timeout=0.1)
            except Empty:
                continue
            
            pressed_keys = [str(k).lower() for k in cmd.get("pressed_keys", [])]
            
            # add shutdown logic here
                
            key, requested = cmd["key"], cmd["requested"]

            element = get_element(gv.data_tree, key)
            if element is None:
                print_out(f"[GUI Worker] Unknown GUI Key Error: '{key}'")
                continue
            
            missing_keys = [k for k in gv.confirmation_keys if k not in pressed_keys]
            write_actuation(key, requested, "user", missing_keys=missing_keys)
                    
                
    except Exception as e:
        if not gv.stop_event.is_set():
            _, _, tb = sys.exc_info()
            print_out(f"[GUI Worker] Initial Write Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
    finally: # write nominal states for all switches
        for key in gv.all_keys: # Populate default values
            element = get_element(gv.data_tree, key)
            default = getattr(element, 'nominal', None)
            if default is not None:
                write_actuation(key, default, "auto", do_print=False)
        print_out(f"[GUI Worker] stopped")
