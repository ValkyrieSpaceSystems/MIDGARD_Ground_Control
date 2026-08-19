import time, csv, os, threading, sys, shutil, re, tomllib, math, pynput
import numpy as np
from queue import Queue, Empty, Full
from datetime import datetime, UTC

from peripheral import Peripheral
from openmct import OpenMCTServer, TelemetryServer, buildOpenMCTjs
from utility import get_nested

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

'''


# Configuration Variables
debug_mode = True
show_server_logs = False
simulated_data = True
do_logging = True # Boolean. Log data to csv
print_switch_changes = True # Boolean. Print switch state changes to python terminal
save_csv = False # Boolean. If False, deletes csv files after run. Should only be False in testing
log_output_dir = '' # String of absolute file path. Directory where to store log files. Will attempt to make directory if it doesn't already exist. If left blank (ie ''), will use logs folder inside current working directory.
confirmation_keys = [pynput.keyboard.Key.shift] # List of single character strings for keys or keyboard.Key for which keys to press to enable switch actuation. Leave empty for no confirmation [DANGEROUS!!!]


peripherals = {
    'Valhala_I':{'interface':'elrs', 'manufacturer':'VSS', 'id':'ASGARD_V0.2'},
    #'nidaq':{'interface':'ethernet', 'manufacturer':'NI', 'id':'1'},
    #'labjack':{'interface':'usb', 'manufacturer':'LabJack', 'id':'1'},
}

stop_event = threading.Event()
abort_state = threading.Event()

for name, args in peripherals.items():
    peripherals[name] = Peripheral(stop_event, name, args)

data_tree, all_keys = buildOpenMCTjs(peripherals)


openmct = OpenMCTServer(stop_event, port=4000, show_logs=show_server_logs)
telemetry = TelemetryServer(stop_event, port=4001, show_logs=show_server_logs)


def shutdown():
    print("\nStopping MIDGARD...")
    stop_event.set() # Runs this to stop everything else from running and to enable the shutdown process
    openmct.stop()
    telemetry.stop()

def incoming_data_handler():
    if simulated_data:
        i = 0
        j = False
        str_list = ['a','b','c','d','e','f','g','h','i','j','k']
        
    while not stop_event.is_set():
        if simulated_data:
            data = {}
            for key in all_keys:
                element = get_nested(data_tree, key.split('.'))
                if element.unit == 'bool': data[key] = j
                elif element.unit == 'str': data[key] = str_list[math.floor(i/10)]
                else: data[key] = i
            i = 0 if i == 100 else i + 1
            j = not j if i == 100 else j
        else:
            pass
            
        #print(data)
        
        for stream_id, value in data.items():
            telemetry.send(stream_id, value, data_tree)
            
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
        
        time.sleep(0.05)

def input_controller():
    # Handles all user inputs from GUI
    
    pressed_keys=[]
    def add_key(key): 
        if key not in pressed_keys: 
            pressed_keys.append(key)
    def remove_key(key): 
        if key in pressed_keys: 
            pressed_keys.remove(key)
    
    with pynput.keyboard.Listener(on_press = add_key, on_release = remove_key) as listener:   
        while not stop_event.is_set():
            if all(key in pressed_keys for key in confirmation_keys):
                #print('Shift Pressed', time.time()*1000)
                pass
                '''def handle_command(key, requested):
                    if key == "rocket1.arm":
                        if requested and not safety_checks_passed():   # your real interlock logic goes here
                            print(f"DENIED: {key} arm request rejected — safety check failed")
                            return False   # tell Python to report the switch as still OFF
                        return requested
                    return requested   # no safety logic defined for this key yet — allow as-is

                telemetry.on_command(handle_command)
                
                
                telemetry.publish("rocket1.arm", False)'''
            time.sleep(0.05)

def main():
    try:
        telemetry.start()
        openmct.start()
        while not telemetry.is_running and not openmct.is_running:
            time.sleep(0.01)
        output_thread = threading.Thread(target=incoming_data_handler)
        output_thread.start()
        input_thread = threading.Thread(target=input_controller)
        input_thread.start()
        # while threads not running wait
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt as e:
        if not stop_event.is_set():
            output_thread.join()
            input_thread.join()
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
    Data display
    Sending data to gui
    

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
        Make units optional for some element types
        Switches, Multi-Select, and Sliders
        Terminal Display
        Features
            Servo Alignment
            Restart frontend while python is running?
    Data Handling
        Recieving data
        Logging data
        Low priority data (bypass, gpio 24/25, diagnostics, etc)
    Actuator Control
        Recieving commands from GUI, checking conditions, sending affirm/deny
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