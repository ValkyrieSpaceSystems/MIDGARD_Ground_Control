import time, csv, os, threading, sys, shutil, re, tomllib, math, pynput, csv, tomllib, importlib.util, asyncio, json, sqlite3, uvicorn, subprocess, urllib.request, urllib.error, socket, platform, serial
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

from peripheral import Peripheral, Actuator
from openmct import OpenMCTServer, TelemetryServer, buildOpenMCTjs
from midgard_functions import gv, print_out, check_and_install_openmct, check_configs, check_peripherals, write_actuation, shutdown, peripheral_worker, gui_worker


'''
prerequisite installed applications: git (https://git-scm.com/install/), Node.js/npm (Windows: https://github.com/nvm-windows/nvm, Mac/Linux: https://nodejs.org/en/download), python 3.12

peripheral folder containing folders for each manufacturer, each manufacturer has files for each ID containing information about the sensors (raw data), processed data, actuators, and other variables. each system and model has a generic file defining system and model specific variables and default values and telling MIDGARD how to communicate with this specific device on different interfaces and what interfaces are valid (optional interfaces will be defined in the other variables setting). system folders can also contain custom sequences for that system and id files can contain whether they're valid

peripheral config files can inherit configs from other files in the structure parent = 'parent_file' for parent_file.toml in a manufacturer or parent = 'parent_folder/file_name' for parent_file.toml in parent_folder manufacturer

peripherals are made of elements (data streams, actuators, lockouts, interfaces, and phases)

for NI each cDAQ is a single peripheral

selectors can only be nominalized if the transition is valid. a selector can only be nominalized if the nominal state is in the current state's transition to. example: while a rocket is in state ascent, it cannot be nominalized to the state pad. 
selector nominal state should have all sources in its sources list because nominalization does not check for sources. 

openmct was built with npm run build and is being served by fastapi through python. sqlite is the telemetry server serving realtime and historical data. 
openmct index.html is overwritten. if custom openmct plugins are needed, add them to the index.html in openmct_midgard. 

toml default is the state midgard writes automatically at startup, a value of false is the nominal/safe position (ie normally open or normally closed is the safe state and configured in hardware to equate with a value of false)

abort and logging actuators cannot have sources

if id and type are the same, the type folder will collapse and become the element object. if id and type are the same, no other elements of that type will be supported for that peripheral

cannot have duplicate ids on the same peripheral

currently, only switches can be tied to a physical actuator, but other control_types can call a function to actuate a switch for them. this can be expanded, but will require more control logic.

if a peripheral calls an actuation, midgard will ignore the elements in to_actuate when commanding the physical actuations (digital actuation will still occur) and rely on the peripheral to make those actuations without midgard needing to instruct them. the operating procedure is to have a duplicate of all actuators on both the peripheral and midgard with the proper configuration so propagated actuations occur the same on both systems (ideally use the same config file) (ie if ASGARD calls for main_lockout set to false, ASGARD and MIDGARD set everything armed by main_lockout False independently, but MIDGARD also sets the other peripherals that rely on what this peripheral actuated)

functions can be tied to any actuator, but buttons, switches, and selectors are recommended (at least for now). sequences just run code and gv.threads will run the function as a thread. buttons will run a sequence every actuation (except when they become inhibited) and gv.threads will be run forever (but cant have any armed or disarmed by and the thread must handle all safety logic) (BUTTON gv.threads ARE NOT RECOMMENDED!). Switches will run a sequence or thread if the value is true. Selectors will run the function if the state has do_function and will run always if no states have do_function (will run the function when nominalization occurs if nominalized state is not inhibited and do_on_nominalization is True)

put in readme: Read the manual. life and limb could be at stake if you fail to understand how midgard actually works

mavlink. for each data stream element you must specify the message type, field name, and sampling rate. mavlink returns data at the message type level, so midgard will select the fastest sampling rate from the elements of the message type. multiple elements can have the same message type and field. the mavlink protocol interface is now supported and must have a device, baud, and timeout specified. mavlink works over any serial device so its interface is type serial with protocol mavlink. this has only been tested with usb sofar, but should work with radios connected through serial

'''


### Configuration Variables ###

# Data
log_data = True # Boolean. Log data to csv
log_actuations = True # Boolean. Log GUI commands to csv
simulated_data = False # Boolean. Discards peripheral data and replaces it with fake data. Meant for testing without actual peripherals/peripheral data.
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
    #'Valhalla_I':{'interface':'elrs', 'manufacturer':'VSS', 'id':'ASGARD_V0.2'},
    'Pixhawk':{'interface':'mavlink', 'manufacturer':'default', 'id':'Pixhawk4'}
    #'nidaq':{'interface':'ethernet', 'manufacturer':'NI', 'id':'1'},
    #'labjack':{'interface':'usb', 'manufacturer':'LabJack', 'id':'1'},
}


### End of Condifuration Variables. Do Not Edit Further ###


'''
# ---------------------------------------------------------------------------
# Connection settings (same as the collector script)
# ---------------------------------------------------------------------------
CONNECTION = "udpin:localhost:14540"
BAUD = 115200

ACK_TIMEOUT_S = 3
ACK_RETRIES = 3


# ---------------------------------------------------------------------------
# Low-level: COMMAND_LONG + ack
# ---------------------------------------------------------------------------
def send_command(master, command, p1=0, p2=0, p3=0, p4=0, p5=0, p6=0, p7=0):
    """
    Send a MAV_CMD_* via COMMAND_LONG and wait for COMMAND_ACK.
    Retries a few times since commands can be dropped, especially over radio.
    Returns True if the autopilot accepted the command.
    """
    try:
        for attempt in range(1, ACK_RETRIES + 1):
            master.mav.command_long_send(
                master.target_system, master.target_component,
                command, 0,
                p1, p2, p3, p4, p5, p6, p7,
            )
            ack = master.recv_match(type="COMMAND_ACK", blocking=True, timeout=ACK_TIMEOUT_S)
            if ack is None:
                print(f"  (attempt {attempt}) no ack for command {command}, retrying")
                continue
            if ack.command != command:
                continue  # a leftover ack for a different command; keep waiting via retry
            ok = ack.result == mavutil.mavlink.MAV_RESULT_ACCEPTED
            result_name = mavutil.mavlink.enums["MAV_RESULT"][ack.result].name
            print(f"command {command} -> {result_name}")
            return ok
        print(f"command {command} -> no ack after {ACK_RETRIES} attempts")
        return False
    except Exception as e:
        _, _, tb = sys.exc_info()
        print(f"{type(e).__name__} on line {tb.tb_lineno}: {e}")


# ---------------------------------------------------------------------------
# Common vehicle commands
# ---------------------------------------------------------------------------
def arm(master, value=True):
    return send_command(master, mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM, 1 if value else 0)


def set_mode(master, mode_name):
    """
    Switch flight mode by name, e.g. 'GUIDED' (ArduPilot) or 'OFFBOARD' (PX4).
    Mode names and numbers differ between firmwares; mode_mapping() picks the
    right table based on the autopilot type seen in the heartbeat.
    """
    try:
        mapping = master.mode_mapping()
        if mapping is None or mode_name not in mapping:
            available = list(mapping.keys()) if mapping else []
            raise ValueError(f"Unknown mode {mode_name!r}. Available: {available}")
        master.set_mode(mapping[mode_name])
        ack = master.recv_match(type="COMMAND_ACK", blocking=True, timeout=ACK_TIMEOUT_S)
        print(f"set_mode({mode_name}) ack: {ack}")
    except Exception as e:
        _, _, tb = sys.exc_info()
        print(f"{type(e).__name__} on line {tb.tb_lineno}: {e}")


def takeoff(master, altitude_m):
    """Climb to `altitude_m` above the arming point. Vehicle must be armed first."""
    return send_command(master, mavutil.mavlink.MAV_CMD_NAV_TAKEOFF, 0, 0, 0, 0, 0, 0, altitude_m)


def land(master):
    return send_command(master, mavutil.mavlink.MAV_CMD_NAV_LAND)


def return_to_launch(master):
    return send_command(master, mavutil.mavlink.MAV_CMD_NAV_RETURN_TO_LAUNCH)


# ---------------------------------------------------------------------------
# Position / velocity setpoints
# ---------------------------------------------------------------------------
# type_mask bits: 1 = ignore this field. Pick a mask matching which fields
# you're actually sending; the rest should be marked ignored.
_USE_POSITION = 0b0000111111111000  # use x, y, z only
_USE_VELOCITY = 0b0000111111000111  # use vx, vy, vz only


def send_position_target_local_ned(master, x, y, z, vx=0, vy=0, vz=0, use_velocity=False):
    """
    Send one local-frame (NED, meters, z-down) setpoint. Only takes effect in
    OFFBOARD/GUIDED mode. Must be called repeatedly (2+ Hz) -- a single call
    will not hold position, and PX4 will not even accept OFFBOARD mode unless
    setpoints are already streaming when you request it.
    """
    try:
        mask = _USE_VELOCITY if use_velocity else _USE_POSITION
        master.mav.set_position_target_local_ned_send(
            0,  # time_boot_ms (0 = autopilot fills it in)
            master.target_system, master.target_component,
            mavutil.mavlink.MAV_FRAME_LOCAL_NED,
            mask,
            x, y, z,
            vx, vy, vz,
            0, 0, 0,   # acceleration x,y,z (unused here)
            0, 0,      # yaw, yaw_rate (unused here)
        )
    except Exception as e:
        _, _, tb = sys.exc_info()
        print(f"{type(e).__name__} on line {tb.tb_lineno}: {e}")


def goto_position_local(master, x, y, z, duration_s, rate_hz=4):
    """
    Stream a position setpoint for `duration_s` seconds at `rate_hz`.
    Call this (or send_position_target_local_ned in your own loop) BEFORE
    and DURING offboard/guided mode -- stopping the stream drops the
    autopilot out of that mode as a failsafe.
    """
    try:
        period = 1.0 / rate_hz
        end = time.time() + duration_s
        while time.time() < end:
            send_position_target_local_ned(master, x, y, z)
            time.sleep(period)
    except Exception as e:
        _, _, tb = sys.exc_info()
        print(f"{type(e).__name__} on line {tb.tb_lineno}: {e}")


# ---------------------------------------------------------------------------
# Custom commands (MAV_CMD_USER_1..5, IDs 31000-31004)
# ---------------------------------------------------------------------------
# Stock PX4/ArduPilot firmware doesn't act on these -- something on the
# vehicle (companion computer, custom firmware, Lua script) must handle them.
def send_custom_command(master, p1=0, p2=0, p3=0, p4=0, p5=0, p6=0, p7=0, cmd=mavutil.mavlink.MAV_CMD_USER_1):
    return send_command(master, cmd, p1, p2, p3, p4, p5, p6, p7)

'''




def main():
    try:
        # Set Global Variables
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

        # Set directories
        if gv.log_output_dir == '':
            gv.log_output_dir = os.path.join(os.path.dirname(__file__), "logs")
        if gv.openmct_dir == '':
            gv.openmct_dir = os.path.join(os.path.dirname(__file__), "openmct")


        # Check Configuration Variables and OpenMCT
        check_configs()
        check_and_install_openmct() 


        # Setup Peripherals
        for name, args in gv.peripherals.items():
            gv.peripherals[name] = Peripheral(name, args) 
            
        for p in gv.peripherals.values():
            for elem in p.actuators.values():
                group = elem.sync_group
                if group:
                    gv.sync_groups.setdefault(group, []).append(elem)

        # Check peripherals
        check_peripherals()
        
        
        # Setup GUI
        buildOpenMCTjs()
        gv.openmct = OpenMCTServer()
        gv.telemetry = TelemetryServer()
        
        
        # Create servers
        gv.openmct.start()
        gv.servers.append(("OpenMCT Server", gv.openmct))
        gv.telemetry.start()
        gv.servers.append(("Telemetry Server", gv.telemetry))
        
        # Create gv.threads        
        for peripheral in gv.peripherals.values():
            peripheral.worker = threading.Thread(target=peripheral_worker, name=peripheral.worker_name, args=(peripheral,))
            peripheral.worker.daemon = True
            peripheral.worker.start()
            gv.threads.append((peripheral.worker_name, peripheral.worker))
        
        gui_init_event = threading.Event()
        gui_handler = threading.Thread(target=gui_worker, name="GUI Worker", args=(gui_init_event,))
        gui_handler.daemon = True
        gui_handler.start()
        gv.threads.append(("GUI Worker", gui_handler))
        
        while not (all([peripheral.init_event.is_set() for peripheral in gv.peripherals.values()]) and gui_init_event.is_set() and gv.openmct.is_running and gv.telemetry.is_running): # wait for all gv.servers and gv.threads to initialize 
            time.sleep(.1)
        
        if 'Valhalla_I' in gv.peripherals: write_actuation(gv.peripherals['Valhalla_I'].elements['flight_phase'].key, 'pad', "auto", do_print=False) ##### TEMP!!! #####
        
        gv.startup_event.set()
        print_out("\n\nMIDGARD Startup Complete\nPress Ctrl+C to stop...\n\n")
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
            
            
            
            '''master = peripherals['Pixhawk'].interface.connection
            
                # --- uncomment what you need -------------------------------------
                
            while True:
                try:
                    val = int(input("Enter Number: "))
                    if val == 1:
                        arm(master)
                    elif val == 2:
                        set_mode(master, "GUIDED")  # ArduPilot
                    elif val == 3:
                        set_mode(master, "OFFBOARD")  # PX4 (stream setpoints first, see note above)
                    elif val == 4:
                        takeoff(master, 10)  # climb to 10 m
                    elif val == 5:
                        goto_position_local(master, 5, 0, -10, duration_s=10)  # NED: z is negative-up
                    elif val == 6:
                        return_to_launch(master)
                    elif val == 7:
                        land(master)
                    elif val == 8:
                        arm(master, False)
                    elif val == 9:
                        send_custom_command(master, p1=1, p2=2, p3=3)
                    elif val == 10:
                        master.close()
                    else:
                        print(f"Unknown command: {val}")
                except Exception as e:
                    _, _, tb = sys.exc_info()
                    print(f"{type(e).__name__} on line {tb.tb_lineno}: {e}")
                    '''
        
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
COMPLETED:
    Make requirements.txt
    Check for internet access/access to specific needed websites
    

TO DO
    Bugs
        Switch states reset if editing?

    Configuration
        Check configurations are valid
            No duplicate ids of the same element class
            Check that peripheral ids are valid
            Check no duplicates in id arrays in toml files in _resolve_inheritance
            Check that no peripherals in python are using the same external device (interface)
            Check that all the members of sync group have the same values (except name, id, description, etc)
            Check sequences use a supported control_type
            Check that actuators have no buttons in armed_by and disarmed_by
            Check that states only have other states in their selector in transition_to
            Check that all peripheral conditions from other peripherals are loaded
            Check that all interface/manufacturer specific values are present in each element
            Check that all peripheral units are convertible
            Check that the peripheral can actually be communicated with
            Check that NI modules type matches between config and system
        Armed and disarmed from another peripheral
        Convert UTC to other time zone (PST, JST, Martian Sols)
        Cooordinate transformations (ex Earth to object orbit frame or object body frame)
        Auto run simulated peripherals (NI, Pixhawk, Docker, etc)
    Overseer
        Restart of MIDGARD or peripheral handling
        Peripheral communication
        Reconnect to peripheral if connection lost
    GUI
        Add cluster control_type
        Button that transmits a config defined thing over serial
        Restart frontend while python is running?
        Ctrl+C in python terminal or configurable command/key in gui to trigger shutdown
        Light mode
        Limit display data rate to 10Hz (limit of human comprehension)
        Color selecting (matching between graphs and orbits)
        Hold to send
        Plugins
            Slider Value Selector
            2D Joystick
            Servo Alignment
            Map (Geofence and Course) (Satellite?)
                Map send long/lat to another command (https://mavlink.io/en/messages/common.html#mav_commands)
            Fuel Bar/Gaugel
            Button with popup window for inputs
            Boolean display indicator
    Data Handling
        Import csv logs into database
    Actuator Control
        Add servo and cluster support to functions
    Object Peripheral
        Types: city, coordinate, target, facility, etc
        City lookup
        Icon on map
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