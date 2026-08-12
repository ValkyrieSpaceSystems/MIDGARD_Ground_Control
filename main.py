import time, csv, os, threading, sys, shutil, re, tomllib, math
import numpy as np
from pynput import keyboard
from queue import Queue, Empty, Full
from datetime import datetime, UTC
from peripheral import Peripheral
from openmct import OpenMCTServer, TelemetryServer

import nidaqmx
from nidaqmx import stream_readers, DaqReadError
from nidaqmx.stream_writers import DigitalSingleChannelWriter, DigitalMultiChannelWriter
from nidaqmx.constants import TerminalConfiguration, AcquisitionType, LineGrouping, WAIT_INFINITELY
import nidaqmx.system
from labjack import ljm




# Configuration Variables
debug_mode = True
simulated_data = True
do_logging = True # Boolean. Log data to csv
print_switch_changes = True # Boolean. Print switch state changes to python terminal
save_csv = False # Boolean. If False, deletes csv files after run. Should only be False in testing
log_output_dir = '' # String of absolute file path. Directory where to store log files. Will attempt to make directory if it doesn't already exist. If left blank (ie ''), will use logs folder inside current working directory.


peripherals = {
    'Valhala_I':{'interface':'elrs', 'manufacturer':'VSS', 'id':'ASGARD_V0.2'},
    #'nidaq':{'interface':'ethernet', 'manufacturer':'NI', 'id':'1'},
    #'labjack':{'interface':'usb', 'manufacturer':'LabJack', 'id':'1'},
}

openmct = OpenMCTServer(port=4000,show_logs=False)
telemetry = TelemetryServer(port=4001,show_logs=False)


stop_event = threading.Event()
abort_state = threading.Event()


for name, args in peripherals.items():
    peripherals[name] = Peripheral(name, args)


# ── Example usage ──────────────────────────────────────────────────────────────

rocket = peripherals['Valhala_I']

'''print(rocket)
print(rocket.phase)
print(rocket.lockouts)
print(rocket.actuators)

# Receive a telemetry packet from the FC
rocket.update_data('accel_z', 45.2)
rocket.update_data('pos_z', 1200.0)

# Operator arms servos
rocket.set_lockout('main_lock', True)
rocket.set_lockout('servo_arm', True)'''

'''
peripheral folder containing folders for each manufacturer, each manufacturer has files for each ID containing information about the sensors (raw data), processed data, actuators, and other variables. each system and model has a gerneric file defining system and model specific variables and default values and telling MIDGARD how to communicate with this specific device on different interfaces and what interfaces are valid (optional interfaces will be defined in the other variables setting). system folders can also contain custom sequences for that system and id files can contain whether they're valid


openmct was built with npm run build and is being served by fastapi through python. sqlite is the telemetry server serving realtime and historical data. 
'''

data_queue = Queue()

def shutdown():
    print("\nStopping MIDGARD...")
    stop_event.set() # Runs this to stop everything else from running and to enable the shutdown process
    openmct.stop()
    telemetry.stop()

def output_controller():
    # Handles all data outputs to GUI
    i = 0
    j = False
    while not stop_event.is_set():
        
        #print(i, j)
        
        data = {
            'rocket1.altitude': i,
            'rocket1.velocity': i-100 if i == 100 else i,
            'rocket1.arm': j,
        }
        
        # populate data with new data
        
        for key, value in data.items():
            telemetry.send(key, value)
        
        time.sleep(0.05)
        i = 0 if i == 100 else i + 1
        j = not j

def input_controller():
    # Handles all user inputs from GUI
    
    pressed_keys=[]
    def add_key(key): 
        if key not in pressed_keys: 
            pressed_keys.append(key)
    def remove_key(key): 
        if key in pressed_keys: 
            pressed_keys.remove(key)
        
    with keyboard.Listener(on_press = add_key, on_release = remove_key) as listener:   
        listener.join()
        if keyboard.Key.shift in pressed_keys:
            print('Shift Pressed')
            
            '''def handle_command(key, requested):
                if key == "rocket1.arm":
                    if requested and not safety_checks_passed():   # your real interlock logic goes here
                        print(f"DENIED: {key} arm request rejected — safety check failed")
                        return False   # tell Python to report the switch as still OFF
                    return requested
                return requested   # no safety logic defined for this key yet — allow as-is

            telemetry.on_command(handle_command)
            
            
            telemetry.publish("rocket1.arm", False)'''

def main():
    try:
        telemetry.start()
        openmct.start()
        output_thread = threading.Thread(target=output_controller)
        output_thread.start()
        while True:
            time.sleep(5)
    except KeyboardInterrupt as e:
        if not stop_event.is_set():
            output_thread.join()
            shutdown()
    
    except Exception as e:
        _, _, tb = sys.exc_info()
        print(f"[Main Thread] Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
        if not stop_event.is_set():
            output_thread.join()
            shutdown()

if __name__ == '__main__':
    main()

'''
COMPLETED:
    Toml parsing
    Detecting shift key to confirm command
    GUI installed
    GUI start and stop
    

TO DO
    Configuration
        Create data structures
        Check configurations are valid
        Remove an item from a parent id file (ie remove id+pyro_motor from ASGARD_V0.2.toml)
        Sequence handling
    Overseer
        Error handling
        Event handling 
        Watchdog
        Shutdown sequence
        Restart of MIDGARD or peripheral handling
        Peripheral communication
    GUI
        Switch toggling
        Data display
        Features
            Servo Alignment
            Restart frontend while python is running
    Data Handling
        Recieving data
        Logging data
        Sending data to gui
        Low priority data (bypass, gpio 24/25, diagnostics, etc)
    Actuator Control
        Recieving commands from GUI
        Checking conditions
        Affirm/deny command
        Actuating
    Writing
        Readme
        How to
        Flight procedure
        Open Source License
    Legacy
        NI
        LabJack

'''