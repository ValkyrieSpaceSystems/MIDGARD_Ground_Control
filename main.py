import time, csv, os, threading, sys, shutil, re, pynput
import numpy as np
from queue import Queue, Empty, Full
from datetime import datetime, UTC
'''import nidaqmx
from nidaqmx import stream_readers, DaqReadError
from nidaqmx.stream_writers import DigitalSingleChannelWriter, DigitalMultiChannelWriter
from nidaqmx.constants import TerminalConfiguration, AcquisitionType, WAIT_INFINITELY
from nidaqmx.constants import LineGrouping
import nidaqmx.system
from labjack import ljm'''


debug_mode = True


class peripheral:
    def __init__(self, interface, manufacturer, model, id):
        pass




peripherals = {
    'rocket':{'interface':'elrs', 'manufacturer':'VSS', 'id':'1'},
    #'nidaq':{'interface':'ethernet', 'manufacturer':'NI', 'ID':'1'},
    #'labjack':{'interface':'usb', 'manufacturer':'LabJack', 'ID':'1'},
}

for perf in peripherals:
    peripherals[perf]['object'] = peripheral(peripherals[perf]['interface'],peripherals[perf]['manufacturer'],peripherals[perf]['id'])


'''

peripheral folder containing folders for each manufacturer, each manufacturer has files for each ID containing information about the sensors (raw data), processed data, actuators, and other variables. each system and model has a gerneric file defining system and model specific variables and default values and telling MIDGARD how to communicate with this specific device on different interfaces and what interfaces are valid (optional interfaces will be defined in the other variables setting). system folders can also contain custom sequences for that system and id files can contain whether they're valid


peripheral class
        
    interface (usb, radio, bluetooth, rs-485)
    
    manufacturer (NI, LabJack, VSS)
    
    model (NI-9485, LJ-T7, ASGARD_V0.2)
    
    id (serial number or other id for a specific configuration, ie ministand)
        
        sensors (raw) (barometer, accelerometer, gyro, magnetometer, GPS)
        states (processed) (position, velocoty, acceleration, heading, tilt, GPS)
            
        actuators
            type (servo, valve, piston, motor, pyro)
            location (module, channel, pin, etc)
            value (open, closed, 90 deg, 0, 1, etc)
            lockouts/conditions
            
        sequences
            lockouts/conditions
            
        telemetry (system) (events, diagnostics)

            
        other variables (ox_vent_cycle, hotfire_delay, etc)






events are boolean, have they occured or not. event prerequisites are ANDs and conflicts are ORs. status is a single str with multiple options
abort returns status to pad
    

'''

def main():
    pass

if __name__ == '__main__':
    main()

'''
COMPLETED:
    
    
TO DO
    Configuration
        Toml parsing
        Create data structures
        Check configurations are valid
        Remove an item from a parent id file (ie remove id+pyro_motor from ASGARD_V0.2.toml)
    Overseer
        Error handling
        Event handling 
        Watchdog
        Shutdown sequence
    GUI
        Switch toggling
        Data display
        Features
            Servo Alignment
    Data Handling
        Recieving data
        Logging data
        Sending data to gui
        Low priority data (bypass, gpio 24/25, diagnostics, etc)
    Actuator Control
        Recieving commands from GUI
        Detecting shift key to confirm command
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