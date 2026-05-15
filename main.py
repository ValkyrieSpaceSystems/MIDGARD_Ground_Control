import time, csv, os, keyboard, threading, sys, shutil, re
import nidaqmx
from nidaqmx import stream_readers, DaqReadError
from nidaqmx.stream_writers import DigitalSingleChannelWriter, DigitalMultiChannelWriter
from nidaqmx.constants import TerminalConfiguration, AcquisitionType, WAIT_INFINITELY
from nidaqmx.constants import LineGrouping
import nidaqmx.system
import numpy as np
from queue import Queue, Empty, Full
import synnax as sy
from datetime import datetime, UTC
from labjack import ljm



'''
Startup Instructions

Open command line terminal or vs code terminal. Run the following command:
synnax start --listen=localhost:9090 --insecure --license-key=#########
Open Synnax desktop app and connect to the default cluster or the one specified in client
Load Synnax control panel (P&ID) and select channels for each switch, indicator, and graph
Set python Configuration Variables
Run python program
(Instead of using the command above, the synnax cluster can also be run as a docker container)
(Instead of connecting with the desktop app, you can also connect directly at the clusters web address (i.e. localhost:9090). Linux users must use this because there is no console for linux)


Stop Instructions

Press ctrl+c in the python terminal or the shutdown button in the synnax app
Close Synnax app
Type stop in the command line window and press enter before closing the terminal



Configuration Explanation 

Terminology: A sensor is any physical sensor conencted to the NI module for logging. Sensors have a type, sensor_type, that can be either 'PT' (pressure transducer), 'TC' (thermocouple), or 'LC' (load cell). Sensor scaling is the way be convert a raw voltage or current value for a sensor into units. The raw sensor valueis multiplied by the multiplier value and this is added to the offset value. Multiplier and offset values are optional, but if either is applied, the raw value will be separately logged in synnax and the log files. A switch is a switch in synnax that controls the state of a valve, lockout, or sequence. A condition is a switch whose value must be true (conditions_true) or false (conditions_false) for another switch to be in an active state. State refers to the value of the switch (synnax is 0/1, but can state can also be T/F, On/Off, etc) and in the initial arrays is used to specify the initial state on startup. A valve is a channel on an ssr relay controlling a solonoid actuated valve (or some other electrical cicuit in special cases). A lockout is a switch who is only a condition. A sequence is a logic based series of automated events run as either a thread or function with the goal to automatically actuate valves or switches or to perform some other action automatically. Channel refers to either the channel on the relevant NI module or the synnax channels for a switch (context dependent). Module/module number refers to the slot number (with adjustment for starting at 0 or 1 in "NI Counting" below) of an NI module in the cDAQ. Nominal is the plain text default state of a valve (Closed/Open or On/Off/something else in special cases). Plain name is the plain text name of a switch (should include the type of switch ie valve, lockout, or sequence). In sequence_array, function_name is the string identifier of the python function associated with that sequence. The function_name_interpreter dictionary is used to convert the function_name into the actual python function. In sequence_array, thread is a boolean that if True, will have the function run as a thread, and if False, will run the function as a normal function. 

NI Counting: We always start counting at 0. NI starts counting at both 0 and 1 depending on the device. The NI chassis start counting slot numbers at 1, so you will need to subtract 1 to determine the module number. NI-9485 SSR Modules start counting their channels at 0, so you won't need to subtract 1, but other modules do start at 1 so make sure to check. If NI starts at 0, start at 0. If NI starts at 1, you will have to subtract 1 from their value to get the value needed by this program. Example: NI chassis slot 4 becomes module 3 because it starts at module 1, but channel 2 stays 2 because it starts at channel 0.

Limitations
Only one NI cDAQ chassis supported. Nothing is configured to distinguish between different cDAQ chassis. To add multi-chassis support, you will need to add a chassis parameter to valve_array, modify how data is handled in NI, and modify how writing to NI is handled. Not difficult, but definetely, annoying
Sensor type of each NI module is hard coded (ie 9205 is PT only)
All NI device names must be in the format cDAQ{cDAQ number}Mod{Module Number} for modules and cDAQ{cDAQ number} for chassis
If running on Linux, follow the instructions in this thread if having issues creating simulated NI hardware (Maybe real hardware too idk) https://forums.ni.com/t5/Multifunction-DAQ/Simulated-devices-on-Linux-stuck-in-quot-Initializing-quot/m-p/4460845
Only the LabJack T7 is supported (this is because we only have the T7, but this can be easily changed in the handle variable and there shouldn't be any programatic reasons to prevent other device from working)
The only supported LabJack channels are FIO0-7 for valves and AIN0-13 for sensors (inclusive)
'''


# Configuration Variables
do_logging = True # Boolean. Log data to csv
print_switch_changes = True # Boolean. Print switch state changes to python terminal
save_csv = False # Boolean. If False, deletes csv files after run. Should only be False in testing
bypass_shift = True # Boolean. Bypasses the check if for shift being pressed. Must be set True if running on a linux machine to bypass the keyboard library because it needs sudo
bypass_continuity_check = True # Boolean. Bypasses the actual value of continuity for testing only
log_output_dir = '' # String of absolute file path. Directory where to store log files. Will attempt to make directory if it doesn't already exist. If left blank (ie ''), will use current working directory.

# Ground System Mode 
operating_mode = 'miniStand' # String. 'miniStand', 'FGS', 'flowStand', or 'torchIgniter' for which ground system you are using

# Synnax Setup
client = sy.Synnax(
    host="localhost",
    port=9090,
    username="synnax",
    password="seldon",
    secure=False
)

# Sensor Units
pt_units = 'psi' # String. What units do you want associated with your scaled values
tc_units = 'celcius' # String. What units do you want associated with your scaled values
lc_units = 'lbf' # String. What units do you want associated with your scaled values


# Sequence Variable Config
# Ox Vent Cycler Configs
ox_vent_close_for = 3 # Integer. Time in seconds the Ox Vent Cycler should stay closed for.
ox_vent_open_for = 120 # Integer. Time in seconds the Ox Vent Cycler should stay open for.

# Hotfire Configs
mfv_lead_time = 0.125 # Float. Amount of time in seconds fuel should flow before oxidizer starts flowing. 
hotfire_burn_time = 15.0 # Float. Amount of time in seconds the engine should burn for.
hotfire_purge_time = 5.0 # Float. Amount of time in seconds nitrogen should purge for.
countdown_step = 0.1 # Float. How often the countdown clock should tick in seconds.
# End Sequence Variable Config


# NI Setup 
cDAQ = 2 # Integer. What cDAQ chassis are you trying to connect to? You can find this in NI MAX. Only one cDAQ chassis is supported at a time. You can use simulated NI hardware for testing by right clicking 'Devices and Interfaces' in NI MAX and clicking 'Create New...'

# Data Acquisition Rates (Be careful, some rates may not work with your hardware. Test the rates you want for a long time before using them in production)
pt_pull_freq = 1000 # Integer. Acquisition frequency of PT sensors in Hz
pt_push_freq = 10 # Integer. Frequency in Hz of accumulated data being pulled from NI and sent to synnax and csv. Preferably a factor of pt_pull_freq
tc_pull_freq = 50 # Integer. Acquisition frequency of TC sensors in Hz
tc_push_freq = 10 # Integer. Frequency in Hz of accumulated data being pulled from NI and sent to synnax and csv. Preferably a factor of tc_pull_freq
lc_pull_freq = 1000 # Integer. Acquisition frequency of LC sensors in Hz
lc_push_freq = 5 # Integer. Frequency in Hz of accumulated data being pulled from NI and sent to synnax and csv. Preferably a factor of lc_pull_freq
# End NI Setup 


# LabJack Setup
sensor_pull_freq = 1000 # Integer. Acquisition frequency of sensors in Hz
sensor_push_freq = 10 # Integer. Frequency in Hz of accumulated data being pulled from NI and sent to synnax 
# End Labjack Setup



# Global Functions - DO NOT EDIT
sensor_array = {} # Make empty dicts in case none configured for this operating_mode
valve_array = {}
lockout_array = {}
sequence_array = {}

terminal_log = client.channels.create( # This creates a virtual channel that we can write a string to and display in a synnax log plot.
    name="terminal_log",
    data_type=sy.DataType.STRING,
    virtual=True,
    retrieve_if_name_exists=True,
)
terminal_writer = client.open_writer(start=sy.TimeStamp.now(), channels=terminal_log.name, enable_auto_commit=True)
def print_out(string): # This print output to the terminal and to the synnax virtual channel
    try:
        print(string)
        terminal_writer.write({terminal_log.name:str(string)})
    except Exception as e:
        _, _, tb = sys.exc_info()
        if type(e).__name__ == 'StreamClosed'  or type(e).__name__ == 'ConnectionClosedError': # if synnax cluster stops unexpectedly
            if not non_abort_shutdown.is_set():
                non_abort_shutdown.set()
                print('CRITICAL ERROR: Synnax Cluster Closed!')
        else:
            error_out(type(e)(f"Print Out Error: {type(e).__name__} on line {tb.tb_lineno}: {e}"))
        return -1
        _, _, tb = sys.exc_info()
        
def error_out(error): # This will print errors to synnax and raise them
    try:
        terminal_writer.write({terminal_log.name:str(error)})
    except Exception as e:
        _, _, tb = sys.exc_info()
        if type(e).__name__ == 'StreamClosed'  or type(e).__name__ == 'ConnectionClosedError': # if synnax cluster stops unexpectedly
            if not non_abort_shutdown.is_set():
                non_abort_shutdown.set()
                print_out('CRITICAL ERROR: Synnax Cluster Closed!')
    raise error
# End Global Funtions


# Synnax Configuration
# Sensor Configuration
# NI Example: 'example': {'sensor_type':'PT', 'module': 0, 'channel': 0, 'multiplier': 1, 'offset': 0.0}
# LabJack Example: 'example': {'sensor_type':'PT', 'channel': 'AIN0', 'multiplier': 1, 'offset': 0.0}

# Valve Configuration 
# NI Example: {'example': {'state': False, 'module': 0, 'channel': 0, 'conditions_true': ['main_lock'], 'conditions_false':['example2'], 'nominal':'Closed', 'plain_name': 'Example Valve'},}
# LabJack Example: {'example': {'state': False, 'channel': 'FIO0', 'conditions_true': ['main_lock'], 'conditions_false':['example2'], 'nominal':'Closed', 'plain_name': 'Example Valve'},}

# Lockout Configuration
# Example: {'example': {'state': False, 'conditions_true': ['main_lock'], 'conditions_false':['example2'], 'plain_name': 'Example Lockout'},}

# Auto Sequence Configuration
# Example: {'example': {'state': False, 'conditions_true': ['main_lock'], 'conditions_false':['example2'], 'function_name':example_function_name, 'thread':True, 'plain_name': 'Example Sequence'},}


if operating_mode == 'FGS': # OUT OF DATE
    valve_array = {
        'fuelFill': {'state': False, 'module': 4, 'channel': 6, 'conditions_true': ['main_lock'], 'plain_name': 'Fuel Fill Relay'},
        'frm': {'state': False, 'module': 4, 'channel': 5, 'conditions_true': ['main_lock'], 'plain_name': 'FRM Relay'},
        'lrm': {'state': False, 'module': 4, 'channel': 4, 'conditions_true': ['main_lock'], 'plain_name': 'LRM Relay'},
        'mov': {'state': False, 'module': 4, 'channel': 3, 'conditions_true': ['main_lock', 'mpva_lock'], 'plain_name': 'MOV Relay'},
        'mfv': {'state': False, 'module': 4, 'channel': 2, 'conditions_true': ['main_lock', 'mpva_lock'], 'plain_name': 'MFV Relay'},
        'heVent': {'state': False, 'module': 4, 'channel': 1, 'conditions_true': ['main_lock'], 'plain_name': 'He Vent Relay'},
        'hePrime': {'state': False, 'module': 4, 'channel': 0, 'conditions_true': ['main_lock'], 'plain_name': 'He Prime Relay'},
        'heFill': {'state': False, 'module': 5, 'channel': 7, 'conditions_true': ['main_lock'], 'plain_name': 'He Fill Relay'},
        'hrm': {'state': False, 'module': 5, 'channel': 6, 'conditions_true': ['main_lock'], 'plain_name': 'HRM Relay'},
        'obv': {'state': False, 'module': 5, 'channel': 5, 'conditions_true': ['main_lock', 'obv_lock'], 'plain_name': 'OBV Relay'},
        'dewerVent': {'state': False, 'module': 5, 'channel': 4, 'conditions_true': ['main_lock'], 'plain_name': 'Dewer Vent Relay'},
        'loxVent': {'state': False, 'module': 5, 'channel': 3, 'conditions_true': ['main_lock'], 'plain_name': 'LOx Vent Relay'},
        'loxFill': {'state': False, 'module': 5, 'channel': 2, 'conditions_true': ['main_lock'], 'plain_name': 'LOx Fill Relay'},
        'auto_term': {'state': False, 'module': -1, 'channel': -1, 'conditions_true': ['main_lock', 'term_lock', 'ign_lock'], 'plain_name': 'Auto Terminal Sequence'},
        'auto_cyc': {'state': False, 'module': -1, 'channel': -1,'conditions_true': ['main_lock', 'cyc_lock'], 'plain_name': 'Auto Cycle Sequence'},
        'igniter': {'state': False, 'module': 5, 'channel': 0, 'conditions_true': ['main_lock', 'term_lock', 'ign_lock'], 'plain_name': 'Igniter'},
    }
    
elif operating_mode == 'miniStand':
    sensor_array = {
        'Fuel_Supply': {'sensor_type':'PT', 'module': 0, 'channel': 0, 'multiplier': 300.0, 'offset': 0.0}, 
        'Ox_Supply': {'sensor_type':'PT', 'module': 0, 'channel': 1, 'multiplier': 300.0, 'offset': 0.0}, 
        'Fuel_Pilot': {'sensor_type':'PT', 'module': 0, 'channel': 2, 'multiplier': 150.0, 'offset': 0.0}, 
        'Ox_Pilot': {'sensor_type':'PT', 'module': 0, 'channel': 3, 'multiplier': 150.0, 'offset': 0.0}, 
        'Fuel_Tank_Top': {'sensor_type':'PT', 'module': 0, 'channel': 4, 'multiplier': 100.0, 'offset': 0.0}, 
        'Ox_Tank_Top': {'sensor_type':'PT', 'module': 0, 'channel': 5, 'multiplier': 100.0, 'offset': 0.0}, 
        'Fuel_Tank_Bottom': {'sensor_type':'PT', 'module': 0, 'channel': 6, 'multiplier': 150.0, 'offset': 0.0}, 
        'Ox_Tank_Bottom': {'sensor_type':'PT', 'module': 0, 'channel': 7, 'multiplier': 100.0, 'offset': 0.0}, 
        'Fuel_Venturi_In_1': {'sensor_type':'PT', 'module': 0, 'channel': 8, 'multiplier': 100.0, 'offset': 0.0}, 
        'Fuel_Venturi_In_2': {'sensor_type':'PT', 'module': 0, 'channel': 9, 'multiplier': 100.0, 'offset': 0.0}, 
        'Fuel_Throat_1': {'sensor_type':'PT', 'module': 0, 'channel': 10, 'multiplier': 100.0, 'offset': 0.0}, 
        'Fuel_Throat_2': {'sensor_type':'PT', 'module': 0, 'channel': 11, 'multiplier': 100.0, 'offset': 0.0}, 
        'Fuel_Out': {'sensor_type':'PT', 'module': 0, 'channel': 12, 'multiplier': 100.0, 'offset': 0.0}, 
        'Ox_Venturi_In_1': {'sensor_type':'PT', 'module': 0, 'channel': 13, 'multiplier': 100.0, 'offset': 0.0}, 
        'Ox_Venturi_In_2': {'sensor_type':'PT', 'module': 0, 'channel': 14, 'multiplier': 100.0, 'offset': 0.0}, 
        'Ox_Throat_1': {'sensor_type':'PT', 'module': 0, 'channel': 15, 'multiplier': 100.0, 'offset': 0.0}, 
        'Ox_Throat_2': {'sensor_type':'PT', 'module': 0, 'channel': 16, 'multiplier': 100.0, 'offset': 0.0}, 
        'Ox_Out': {'sensor_type':'PT', 'module': 0, 'channel': 17, 'multiplier': 100.0, 'offset': 0.0}, 
        'W': {'sensor_type':'PT', 'module': 0, 'channel': 18}, 
        'X': {'sensor_type':'PT', 'module': 0, 'channel': 19}, 
        'Y': {'sensor_type':'PT', 'module': 0, 'channel': 20}, 
        'Z': {'sensor_type':'PT', 'module': 0, 'channel': 21}, 
        'A': {'sensor_type':'PT', 'module': 0, 'channel': 22}, 
        'B': {'sensor_type':'PT', 'module': 0, 'channel': 23}, 
        'Main_Fuel_Line': {'sensor_type':'PT', 'module': 0, 'channel': 27, 'multiplier': 99.1, 'offset': 2.25}, 
        'Main_Ox_Line': {'sensor_type':'PT', 'module': 0, 'channel': 28, 'multiplier': 100.0, 'offset': 0.0305}, 
        'Feedback_MFV': {'sensor_type':'PT', 'module': 0, 'channel': 29,}, 
        'Feedback_MOV': {'sensor_type':'PT', 'module': 0, 'channel': 30,}, 
        'Igniter_Cont': {'sensor_type':'PT', 'module': 0, 'channel': 31,}, 
        'Fuel_Feed': {'sensor_type':'PT', 'module': 2, 'channel': 0, 'multiplier': 62406.0, 'offset': -264.0}, 
        'Ox_Feed': {'sensor_type':'PT', 'module': 2, 'channel': 1, 'multiplier': 62320.0, 'offset': -263.0}, 
        'Fuel_Manifold': {'sensor_type':'PT', 'module': 2, 'channel': 2, 'multiplier': 62271.0, 'offset': -263.0}, 
        'Ox_Manifold': {'sensor_type':'PT', 'module': 2, 'channel': 3, 'multiplier': 62130.0, 'offset': -261.0}, 
        'PNU': {'sensor_type':'PT', 'module': 2, 'channel': 4, 'multiplier': 62256.0, 'offset': -263.0}, 
        'H': {'sensor_type':'PT', 'module': 2, 'channel': 5, 'multiplier': 62500.0, 'offset': -264.0}, 
        'I': {'sensor_type':'PT', 'module': 2, 'channel': 6, 'multiplier': 62500.0, 'offset': -264.0}, 
        'J': {'sensor_type':'PT', 'module': 2, 'channel': 7, 'multiplier': 62500.0, 'offset': -264.0},
        'TC0': {'sensor_type':'TC', 'module': 7, 'channel': 0}, 
        'TC1': {'sensor_type':'TC', 'module': 7, 'channel': 1}, 
        'TC2': {'sensor_type':'TC', 'module': 7, 'channel': 2}, 
        'TC3': {'sensor_type':'TC', 'module': 7, 'channel': 3}, 
        'TC4': {'sensor_type':'TC', 'module': 7, 'channel': 4}, 
        'TC5': {'sensor_type':'TC', 'module': 7, 'channel': 5}, 
        'TC6': {'sensor_type':'TC', 'module': 7, 'channel': 6}, 
        'TC7': {'sensor_type':'TC', 'module': 7, 'channel': 7},
        'LC0': {'sensor_type':'LC', 'module': 3, 'channel': 0}, 
        'LC1': {'sensor_type':'LC', 'module': 3, 'channel': 1},
    }
    
    valve_array = {
        'fuelPres': {'state': False, 'module': 4, 'channel': 0, 'conditions_true': ['main_lock'], 'conditions_false':[], 'nominal':'Closed', 'plain_name': 'Fuel Pres Valve'},
        'oxPres': {'state': False, 'module': 4, 'channel': 1, 'conditions_true': ['main_lock'], 'conditions_false':[], 'nominal':'Closed', 'plain_name': 'Ox Pres Valve'},
        'fuelVent': {'state': False, 'module': 4, 'channel': 2, 'conditions_true': ['main_lock'], 'conditions_false':[], 'nominal':'Open', 'plain_name': 'Fuel Vent Valve'},
        'oxVent': {'state': False, 'module': 4, 'channel': 3, 'conditions_true': ['main_lock'], 'conditions_false':[], 'nominal':'Open', 'plain_name': 'Ox Vent Valve'},
        'fuelFill': {'state': False, 'module': 4, 'channel': 4, 'conditions_true': ['main_lock'], 'conditions_false':[], 'nominal':'Closed', 'plain_name': 'Fuel Fill Valve'},
        'oxFill': {'state': False, 'module': 4, 'channel': 5, 'conditions_true': ['main_lock'], 'conditions_false':[], 'nominal':'Closed', 'plain_name': 'Ox Fill Valve'},
        'mfv': {'state': False, 'module': 5, 'channel': 0, 'conditions_true': ['main_lock', 'mpva_lock'], 'conditions_false':[], 'nominal':'Closed', 'plain_name': 'MFV Valve'},
        'mov': {'state': False, 'module': 5, 'channel': 1, 'conditions_true': ['main_lock', 'mpva_lock'], 'conditions_false':[], 'nominal':'Closed', 'plain_name': 'MOV Valve'},
        'n2Purge': {'state': False, 'module': 5, 'channel': 2, 'conditions_true': ['main_lock'], 'conditions_false':[], 'nominal':'Closed', 'plain_name': 'N2 Purge Valve'},
        'igniter': {'state': False, 'module': 5, 'channel': 7, 'conditions_true': ['main_lock', 'ign_lock'], 'conditions_false':[], 'nominal':'Off', 'plain_name': 'Igniter'},
    }
    
    lockout_array = {
        #'abort':{'state':True, 'conditions_true': [], 'conditions_false':[], 'plain_name':'Abort Switch'}, # Abort at top so it is the first checked. To disable digital abort, comment out this line
        'main_lock':{'state':False, 'conditions_true': [], 'conditions_false':[], 'plain_name':'Main Lockout'},
        'obv_lock':{'state':False, 'conditions_true': ['main_lock'], 'conditions_false':[], 'plain_name':'OBV Lockout'}, 
        'mpva_lock':{'state':False, 'conditions_true': ['main_lock'], 'conditions_false':[], 'plain_name':'MPVA Lockout'},
        'ox_cycler_lock':{'state':False, 'conditions_true': ['main_lock'], 'conditions_false':['hotfire_lock'], 'plain_name':'Ox Vent Lockout'}, 
        'hotfire_lock':{'state':False, 'conditions_true': ['main_lock', 'ign_lock'], 'conditions_false':['ox_cycler_lock'], 'plain_name':'Hotfire Lockout'},
        'ign_lock':{'state':False, 'conditions_true': ['main_lock'], 'conditions_false':[], 'plain_name':'Igniter Lockout'},
        }

    sequence_array = {
        'dual_mpvas':{'state': False, 'conditions_true':['main_lock', 'mpva_lock'], 'conditions_false':[], 'function_name':'dual_mpva_seq', 'thread':False, 'plain_name':'Dual MPVAs Sequence'},
        'oxVentCycler':{'state': False, 'conditions_true':['main_lock', 'ox_cycler_lock'], 'conditions_false':['hotfire'], 'function_name':'ox_vent_cycler_seq', 'thread':True, 'plain_name':'Ox Vent Cycler Sequence'},
        'hotfire':{'state': False, 'conditions_true':['main_lock', 'mpva_lock', 'hotfire_lock', 'ign_lock', 'logging'], 'conditions_false':['oxVentCycler'], 'function_name':'hotfire_seq', 'thread':True, 'plain_name':'Hotfire Sequence'},
        'logging':{'state': False, 'conditions_true':[], 'conditions_false':[], 'function_name':'logging_thread', 'thread':True, 'plain_name':'Logging'},
    } 
    
elif operating_mode == 'flowStand':
    sensor_array = {
        'AIN0': {'sensor_type':'PT', 'channel': 'AIN0', 'multiplier': 1, 'offset': 0.0},
        'AIN1': {'sensor_type':'PT', 'channel': 'AIN1', 'multiplier': 1, 'offset': 0.0},
        'AIN2': {'sensor_type':'PT', 'channel': 'AIN2', 'multiplier': 1, 'offset': 0.0},
        'AIN3': {'sensor_type':'PT', 'channel': 'AIN3', 'multiplier': 1, 'offset': 0.0},
        'AIN4': {'sensor_type':'PT', 'channel': 'AIN4', 'multiplier': 1, 'offset': 0.0},
        'AIN5': {'sensor_type':'PT', 'channel': 'AIN5', 'multiplier': 1, 'offset': 0.0},
    }
    
    valve_array = {
        'fio0': {'state': False, 'channel': 'FIO0', 'conditions_true': ['main_lock'], 'conditions_false':[], 'nominal':'Closed', 'plain_name': 'FIO0 Valve'},
        'fio1': {'state': False, 'channel': 'FIO1', 'conditions_true': ['main_lock'], 'conditions_false':[], 'nominal':'Closed', 'plain_name': 'FIO1 Valve'},
        'fio2': {'state': False, 'channel': 'FIO2', 'conditions_true': ['main_lock'], 'conditions_false':[], 'nominal':'Closed', 'plain_name': 'FIO2 Valve'},
        'fio3': {'state': False, 'channel': 'FIO3', 'conditions_true': ['main_lock'], 'conditions_false':[], 'nominal':'Closed', 'plain_name': 'FIO3 Valve'},
        'fio4': {'state': False, 'channel': 'FIO4', 'conditions_true': ['main_lock'], 'conditions_false':[], 'nominal':'Closed', 'plain_name': 'FIO4 Valve'},
        'fio5': {'state': False, 'channel': 'FIO5', 'conditions_true': ['main_lock'], 'conditions_false':[], 'nominal':'Closed', 'plain_name': 'FIO5 Valve'},
        'fio6': {'state': False, 'channel': 'FIO6', 'conditions_true': ['main_lock'], 'conditions_false':[], 'nominal':'Closed', 'plain_name': 'FIO6 Valve'},
    }
    
    lockout_array = {
        'main_lock':{'state':False, 'conditions_true': [], 'conditions_false':[], 'plain_name':'Main Lockout'},
    }
    
    sequence_array = {
        'logging':{'state': False, 'conditions_true':[], 'conditions_false':[], 'function_name':'logging_thread', 'thread':True, 'plain_name':'Logging'},
    }
    
elif operating_mode == 'torchIgniter':
    lockout_array = {
        'abort':{'state':True, 'conditions_true': [], 'conditions_false':[], 'plain_name':'Abort Switch'}, # Abort at top so it is the first checked. To disable digital abort, comment out this line
    }
    
else: error_out(ValueError(f"operating_mode variable is '{operating_mode}'. Must be 'FGS', 'miniStand', 'flowStand', or 'torchIgniter'"))

# Put auto sequence functions here
# Sequences use the valve_controller synnax writer
def dual_mpva(writer): # Actuate both MPVAs with the propper lead time
    mfv_inhibited = any(not bool(status[cond]['value']) for cond in master_array['mfv']['conditions_true']) or any(status[cond]['value'] for cond in master_array['mfv']['conditions_false'])
    mov_inhibited = any(not bool(status[cond]['value']) for cond in master_array['mov']['conditions_true']) or any(status[cond]['value'] for cond in master_array['mov']['conditions_false'])
    
    if not mfv_inhibited and not mov_inhibited and status['mfv']['value'] == status['mov']['value']: # Prevents the MPVAs from being actuated if they're inhibited or in different positions (ie one on and one off)
        if not abort_state.is_set(): # Doing this instead of calling write_single_switch because of more precise mpva_lead_times
            if daq_system == 'NI':
                valve_states = [[False,False,False,False,False,False,False,False]] * len(ssr_mods)
                for switch in valve_array: # build valve_states
                    valve_states[valve_array[switch]['ssr_mod']][valve_array[switch]['channel']] = bool(status[switch]['value'])
                
                valve_states[valve_array['mfv']['ssr_mod']][valve_array['mfv']['channel']] = bool(status['dual_mpvas']['value'])
                #start = time.perf_counter()
                ssr_writers[valve_array['mfv']['ssr_mod']].write_one_sample_one_line(np.array(valve_states[valve_array['mfv']['ssr_mod']]))
                
                time.sleep(mfv_lead_time)
                
                valve_states[valve_array['mov']['ssr_mod']][valve_array['mov']['channel']] = bool(status['dual_mpvas']['value'])                
                ssr_writers[valve_array['mov']['ssr_mod']].write_one_sample_one_line(np.array(valve_states[valve_array['mov']['ssr_mod']]))
                #end = time.perf_counter()
                #print(start,end,end - start)
                
            elif daq_system == 'LabJack':
                writer.write({status['mfv']['state_time']:sy.TimeStamp.now(),status['mfv']['state']:status['dual_mpvas']['value']}) # before for more precise lead time
                ljm.eWriteName(handle, valve_array['mfv']['channel'], status['dual_mpvas']['value']) 
                
                time.sleep(mfv_lead_time)
                
                ljm.eWriteName(handle, valve_array['mov']['channel'], status['dual_mpvas']['value']) 
                writer.write({status['mov']['state_time']:sy.TimeStamp.now(),status['mov']['state']:status['dual_mpvas']['value']}) # after for more precise lead time
            
            
    else: 
        mpva_error = ''
        if mfv_inhibited or mov_inhibited: 
            blocked = [master_array[condition]['plain_name'] for condition in master_array['mfv']['conditions_true'] if bool(status[condition]['value']) == False] + [master_array[condition]['plain_name'] for condition in master_array['mfv']['conditions_false'] if bool(status[condition]['value']) == True] + [master_array[condition]['plain_name'] for condition in master_array['mov']['conditions_true'] if bool(status[condition]['value']) == False] + [master_array[condition]['plain_name'] for condition in master_array['mov']['conditions_false'] if bool(status[condition]['value']) == True]
            mpva_error = mpva_error + f'Inhibited by {list(set(blocked))}. ' # A list of all inhbiiting cnditions
        if status['mfv']['value'] != status['mov']['value']:
            mpva_error = mpva_error + f'Blocked by differing MPVA states (mfv: {status['mfv']['value']}, mov: {status['mov']['value']}). '
        print_out(f'MPVAs not actuated. {mpva_error.rstrip()}')
        # Resets dual_mpva
        status['dual_mpvas']['value'] = int(not bool(status['dual_mpvas']['value']))
        writer.write({status['dual_mpvas']['state']: status['dual_mpvas']['value'], status['dual_mpvas']['state_time']: sy.TimeStamp.now()})

def ox_vent_cycler(writer): # Cycle ox vent open and closed to prevent freezing of vent valve
    print_out(f'Starting Ox Vent Cycler')
    event = status['oxVentCycler']['event']
    
    try: # NOTE: Ox Vent in normally open (0 = open, 1 = closed)!!!!!
        # Start Open
        status['oxVent']['value'] = 1 # We want to start with the valve open, so, without atually actuating it, we set its state to closed so the loop will actuate it to the open position first
        while not stop_event.is_set() and not abort_state.is_set() and not event.is_set(): #
            if bool(status['oxVent']['value']): # if currently closed, then open for
                wait_for = ox_vent_open_for
            else: # if currently open, then close for
                wait_for = ox_vent_close_for
            start = time.perf_counter()
            write_single_switch(writer, 'oxVent', int(not bool(status['oxVent']['value']))) # switch valve state
            wait = time.perf_counter() - start
            while not (wait > wait_for - start) and not stop_event.is_set() and not abort_state.is_set() and not event.is_set(): # While not enough time has elapsed and while it shouldn't stop
                time.sleep(.05)
                wait = time.perf_counter() - start
    except Exception as e:
        _, _, tb = sys.exc_info()
        print_out(f"Ox Vent Cycler Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
    finally:
        if not stop_event.is_set() and not abort_state.is_set():
            write_single_switch(writer, 'oxVent', 0) # Finish Open
        print_out(f'Stopping Ox Vent Cycler') # I left this print in so we know that any running cycler threads stop after a lockout inhibits them

def countdown(writer, event): # Special, not a sequence function. Called by hotfire sequence instead of a synnax switch
    try:
        global countdown_clock
        start_time = time.time()
        next_time = start_time
        while not stop_event.is_set() and not abort_state.is_set() and not event.is_set():
            countdown_clock += countdown_step
            next_time += countdown_step
            time.sleep(max(0, next_time - time.time()))
            writer.write({'countdown_time':sy.TimeStamp.now(),'countdown_channel':abs(countdown_clock)})
            
    except Exception as e:
        _, _, tb = sys.exc_info()
        print_out(f"Countdown Timer Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")

def hotfire(writer): # Run sequence of events for a hotfire
    print_out(f'Starting Hotfire Sequence')
    event = status['hotfire']['event']
    global countdown_clock

    def hotfire_abort(cause): 
        global countdown_clock
        if not event.is_set():
            event.set()
        close_list = ['igniter','dual_mpvas','n2Purge','hotfire_lock','ign_lock','logging'] # Switches to turn off on hotfire abort, dual_mpva called to prevent hard stop and because mfv and mov will never be in different states when abort could be called
        for switch in close_list:
            write_single_switch(writer, switch, 0)
        countdown_event.set() # Stop countdown and reset
        status['hotfire']['value'] = 0
        countdown_thread.join()
        threads.remove(('Countdown Timer Thread',countdown_thread))
        
        if cause != '': 
            print_out(f'Hotfire Sequence Abort at t{'-' if countdown_clock <=0 else '+'}{abs(countdown_clock):.2f} seconds: {cause}')
        countdown_clock = -10
        
        writer.write({'countdown_time':sy.TimeStamp.now(),'countdown':abs(countdown_clock),status['hotfire']['state']:0,status['hotfire']['state_time']:sy.TimeStamp.now()})
        
    try:
        # Run conditions
        # Add tests for dual_mpvas, mfv, mov, n2purge, igniter states
        
        
        # T-10
        
        # Start Countdown
        countdown_event = threading.Event()
        countdown_thread = threading.Thread(target=countdown, args=(writer, countdown_event), name='Countdown Timer Thread')
        countdown_thread.start()
        threads.append(('Countdown Timer Thread',countdown_thread))
        
        # Test for continuity for 5 sec
        print_out('Hotfire Sequence: Running Continuity Check')
        with continuity_queue.mutex:
            continuity_queue.queue.clear()
        
        while not (countdown_clock >= -5) and not stop_event.is_set() and not abort_state.is_set() and not event.is_set():
            time.sleep(.01)
        if stop_event.is_set() or abort_state.is_set() or event.is_set():
            hotfire_abort('Hotfire Deactivated')
            return
        
        # T-5
        
        continuities = []
        while not continuity_queue.empty():
            continuities.append(continuity_queue.get())
        if continuities: # make sure continuity was detected
            avg_continuity = sum(continuities) / len(continuities)
            print_out(f"Hotfire Sequence: Average during continuity check: {avg_continuity:.2f} V")
            
            if bypass_continuity_check:
                avg_continuity = 5 ##################################### for simulated testing only 
            
            if avg_continuity <= 1:
                hotfire_abort("No continuity detected in first 5 seconds")
                return
        else:
            hotfire_abort("No continuity data received during continuity check")
            return

        write_single_switch(writer, 'igniter', 1, False)
        print_out('Hotfire Sequence: Igniter On')

        # Test again for continuity breaking
        print_out('Hotfire Sequence: Running Continuity Check')
        with continuity_queue.mutex:
            continuity_queue.queue.clear()
        
        while not (countdown_clock >= 0) and not stop_event.is_set() and not abort_state.is_set() and not event.is_set():
            time.sleep(.01)
        if stop_event.is_set() or abort_state.is_set() or event.is_set():
            hotfire_abort('Hotfire Deactivated')
            return
        
        # T-0
        
        continuities = []
        while not continuity_queue.empty():
            continuities.append(continuity_queue.get())
        if continuities: # make sure continuity broke
            has_loss = any(v < 1 for v in continuities)
            print_out(f"Hotfire Sequence: Continuity min during igniter fire check: {min(continuities):.2f} V")
            
            if bypass_continuity_check:
                has_loss = True ##################################### for simulated testing only 
            
            if not has_loss: # if not wait for 10 sec for it to break then continue
                print_out("Hotfire Sequence: No continuity loss in second 5s window — extending for 10s")
                extended_start = time.time()
                loss_detected = False

                while time.time() - extended_start < 10.0:
                    if not continuity_queue.empty():
                        pt_val = continuity_queue.get()
                        if pt_val < 1: # continuity broken
                            print_out(f"Hotfire Sequence: Continuity lost at {time.time()-extended_start:.2f}s into extended window")
                            loss_detected = True
                            time.sleep(1.0) # wait 1 second after break
                            break
                    time.sleep(0.1)
                    
                    if stop_event.is_set() or abort_state.is_set() or event.is_set():
                        hotfire_abort('Hotfire Deactivated')
                        return
                    

                if not loss_detected:
                    hotfire_abort("Continuity not lost in extended 10s window")
                    return

        write_single_switch(writer, 'igniter', 0, False)
        print_out('Hotfire Sequence: Igniter Off')
        
        # Begin Burn
        print_out('Hotfire Sequence: MFV On') # Putting this before to minimize deviation from mfv_lead_time
        write_single_switch(writer, 'dual_mpvas', 1, False)
        burn_start_clock = countdown_clock
        burn_start_time = time.time()
        print_out('Hotfire Sequence: MOV On')
        print_out(f"Hotfire Sequence: BURNING at t{'-' if burn_start_clock <=0 else '+'}{abs(burn_start_clock):.2f} seconds for {hotfire_burn_time:.2f} seconds")
        
        while (time.time() - burn_start_time < hotfire_burn_time) and not stop_event.is_set() and not abort_state.is_set() and not event.is_set(): # wait for burn time
            time.sleep(.01)
        if stop_event.is_set() or abort_state.is_set() or event.is_set():
            hotfire_abort('Hotfire Deactivated')
            return
        
        # Stop Burn
        write_single_switch(writer, 'dual_mpvas', 0, False)
        burn_stop_clock = countdown_clock
        burn_stop_time = time.time()
        print_out('Hotfire Sequence: MFV Off') # Putting this after to minimize delays from printing
        print_out('Hotfire Sequence: MOV Off')
        print_out(f"Hotfire Sequence: BURN COMPLETE at t{'-' if burn_stop_clock <=0 else '+'}{abs(burn_stop_clock):.2f} seconds. Burned for {(burn_stop_clock - burn_start_clock):.2f} ({(burn_stop_time - burn_start_time):.2f}) seconds")
        
        while (time.time() - burn_stop_time < 5) and not stop_event.is_set() and not abort_state.is_set() and not event.is_set(): # wait for 5 seconds before purge
            time.sleep(.01)
        if stop_event.is_set() or abort_state.is_set() or event.is_set():
            hotfire_abort('Hotfire Deactivated')
            return
        
        # Purge
        write_single_switch(writer, 'n2Purge', 1, False)
        print_out('Hotfire Sequence: N2 Purge On')
        
        start_time = time.time()
        while (time.time() - start_time < hotfire_purge_time) and not stop_event.is_set() and not abort_state.is_set() and not event.is_set(): # wait for purge time
            time.sleep(.01)
        if stop_event.is_set() or abort_state.is_set() or event.is_set():
            hotfire_abort('Hotfire Deactivated')
            return
            
        # Stop purge
        write_single_switch(writer, 'n2Purge', 0, False)
        print_out('Hotfire Sequence: N2 Purge Off')
        print_out('Hotfire Sequence Complete')
        
        # Close lockouts
        event.set()
        write_single_switch(writer, 'mpva_lock', 0, False)
        write_single_switch(writer, 'hotfire_lock', 0, False)
        write_single_switch(writer, 'ign_lock', 0, False)

        # Stop countdown and hotfire sequence
        countdown_event.set()
        countdown_thread.join()
        threads.remove(('Countdown Timer Thread',countdown_thread))
        write_single_switch(writer, 'logging', 0)
        writer.write({status['hotfire']['state']:0,status['hotfire']['state_time']: sy.TimeStamp.now()})
        print_out(f'Stopping Hotfire Sequence  at t{'-' if burn_stop_clock <=0 else '+'}{abs(burn_stop_clock):.2f} seconds') 

        return
    
    except Exception as e:
        hotfire_abort('')
        _, _, tb = sys.exc_info()
        print_out(f"Hotfire Sequence Error: {type(e).__name__} on line {tb.tb_lineno}: {e}") 

def logging(writer):
    event = status['logging']['event']
    
    # Get sensor names and scaling info
    pt_scaling, pt_names, pt_time_channel, pt_raw_channels, pt_scaled_channels = build_sensor_data('PT', False)
    tc_scaling, tc_names, tc_time_channel, tc_raw_channels, tc_scaled_channels = build_sensor_data('TC', False)
    lc_scaling, lc_names, lc_time_channel, lc_raw_channels, lc_scaled_channels = build_sensor_data('LC', False)
    
    # Create the csv headers
    pt_headers = ["unix_time_ns"] + [f"PT_{pt_units}_{pt_names[i]}" for i in range(len(pt_names)) if pt_names[i] != None] + [f"PT_raw_{pt_names[i]}" for i in range(len(pt_names)) if pt_names[i] != None and pt_scaling[i] != None]
    tc_headers = ["unix_time_s"] + [f"TC_{tc_units}_{tc_names[i]}" for i in range(len(tc_names)) if tc_names[i] != None] + [f"TC_raw_{tc_names[i]}" for i in range(len(tc_names)) if tc_names[i] != None and tc_scaling[i] != None]
    lc_headers = ["unix_time_s"] + [f"LC_{lc_units}_{lc_names[i]}" for i in range(len(lc_names)) if lc_names[i] != None] + [f"LC_raw_{lc_names[i]}" for i in range(len(lc_names)) if lc_names[i] != None and lc_scaling[i] != None]
    
    # get number of sensors and acquisition frequencies
    num_pt_chan = sum(1 for v in sensor_array.values() if v.get('sensor_type') == 'PT')
    num_tc_chan = sum(1 for v in sensor_array.values() if v.get('sensor_type') == 'TC')
    num_lc_chan = sum(1 for v in sensor_array.values() if v.get('sensor_type') == 'LC')
    if daq_system == 'NI':
        global pt_pull_freq
        global pt_push_freq
        global tc_pull_freq
        global tc_push_freq
        global lc_pull_freq
        global lc_push_freq
    elif daq_system == 'LabJack':
        pt_pull_freq = sensor_pull_freq
        pt_push_freq = sensor_push_freq
        tc_pull_freq = sensor_pull_freq
        tc_push_freq = sensor_push_freq
        lc_pull_freq = sensor_pull_freq
        lc_push_freq = sensor_push_freq
    
    def open_new_file_set(): #Open a new set of timestamped CSVs and writers, write headers, return dicts.
        ts_str = time.strftime("%Y-%m-%d_%H-%M-%S")
        pt_name = f"pt_log_{ts_str}.csv"
        tc_name = f"tc_log_{ts_str}.csv"
        lc_name = f"lc_log_{ts_str}.csv"
        
        filenames = [pt_name, tc_name, lc_name]
        
        # Make files, make writers, and write headers
        files = {}
        writers = {}
        if num_pt_chan != 0: # Dont create files for a sensor if there are no sensors of that type
            files['PT'] = open(f'{log_output_dir}/{pt_name}', 'a', newline='', buffering=1<<16)
            writers['PT'] = csv.writer(files['PT'])
            writers["PT"].writerow(pt_headers)
            csv_files.append(f'{log_output_dir}/{pt_name}')
        if num_tc_chan != 0:
            files['TC'] = open(f'{log_output_dir}/{tc_name}', 'a', newline='', buffering=1<<15)
            writers['TC'] = csv.writer(files['TC'])
            writers["TC"].writerow(tc_headers)
            csv_files.append(f'{log_output_dir}/{tc_name}')
        if num_lc_chan != 0:
            files['LC'] = open(f'{log_output_dir}/{lc_name}', 'a', newline='', buffering=1<<15)
            writers['LC'] = csv.writer(files['LC'])
            writers["LC"].writerow(lc_headers)
            csv_files.append(f'{log_output_dir}/{lc_name}')

        print_out(f'Log Files: {filenames}')

        return files, writers, filenames
    
    flush_counter = 0
    
    print_out(f'Logging Started') 
    
    try:
        if do_logging:
            files, writers, filenames = open_new_file_set()
            
            while not stop_event.is_set() and not event.is_set():
                while not csv_queue.empty(): # write all data from csv_queue until it is empty
                    try:
                        data = csv_queue.get(timeout=1.0)
                    except Empty:
                        continue

                    # Handle rotation request
                    if data.get("kind") == "ROTATE":
                        for fh in files.values():
                            try:
                                fh.close()
                            except:
                                pass
                        files, writers = open_new_file_set()
                        flush_counter = 0
                        continue

                    # Normal data
                    kind = data.get('kind')  # 'PT', 'TC', or 'LC'
                    if kind not in ['PT', 'TC', 'LC']:
                        print_out(f"CSV: unknown kind {kind}")
                        continue

                    try:
                        write_log = False
                        if kind == 'PT': # Define settings for each sensor
                            if num_pt_chan != 0: # Skip if no sensors of that type, this shouldn't occur but is an extra saftey check
                                write_log = True
                                num_samples = round(pt_pull_freq/pt_push_freq)
                                time_multiplier = (1/pt_pull_freq) * 1e9
                        elif kind == 'TC':
                            if num_tc_chan != 0: 
                                write_log = True
                                num_samples = round(tc_pull_freq/tc_push_freq)
                                time_multiplier = (1/tc_pull_freq) * 1e9
                        elif kind == 'LC':
                            if num_lc_chan != 0: 
                                write_log = True
                                num_samples = round(lc_pull_freq/lc_push_freq)
                                time_multiplier = (1/lc_pull_freq) * 1e9
                        
                        if write_log:
                            ts_end = int(data['time_ns'])  # the timestamp we get is in the synnax time format, so we convert to an integer based time standard for logging
                            for i, sample in enumerate(data['data']): # the timestamp we get is from the last data point so we need to calculate the timestamps backwards from this
                                ts = ts_end - (num_samples - i) * time_multiplier # last sample is latest time
                                writers[kind].writerow([ts] + sample)
                            
                            flush_counter += 1
                            if flush_counter % 50 == 0: # if the remaineder of flush_counter/50 == 0, aka every 50 writes, flush the file (aka update the file with writes)
                                for f in files.values():
                                    f.flush()

                    except Exception as e:
                        _, _, tb = sys.exc_info()
                        print_out(f"Logging row error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
                        continue
                time.sleep(.1) # prevents the while loop from causing errors if the queue is empty
                    
        else:
            while not stop_event.is_set() and not abort_state.is_set() and not event.is_set(): # prevents the sequence from failing if do_logging is false
                time.sleep(.5)
                
    except Exception as e:
        _, _, tb = sys.exc_info()
        print_out(f"Logging Setup Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
        
    finally:
        print_out(f'Logging Stopped') 
        for fh in files.values():
            try:
                fh.close()
            except:
                pass

# Put the function associated switch each function_name here
# Example: {'example_function_name':example_function}
function_name_interpreter ={ # The 'function_name' key in sequences is needed so the sequence functions can be independent of the operating mode configuration and below it in code
    'dual_mpva_seq':dual_mpva,
    'ox_vent_cycler_seq':ox_vent_cycler,
    'hotfire_seq':hotfire,
    'logging_thread':logging
}

abort_independent_switches = ['logging'] # any switches who should not be constrained by abort (ex logging)

# End of User Configuration





# Defining global variables
for seq in sequence_array: # add the functions to each sequence
    try:
        sequence_array[seq]['function'] = function_name_interpreter[sequence_array[seq]['function_name']]
    except Exception as e:
        error_out(LookupError(f"{seq} sequence 'function_name' not found in function_name_interpreter"))
        
master_array = lockout_array | valve_array | sequence_array # merge lockout, valve, and sequence arrays into one for use with synnax switches, ordered based on priority. Lockouts affect everything and some sequences rely on specific valve states when they run
ni_tasks = [] # Define other global variables
ssr_mods = []
ssr_writers = []
pt_channel_order = []
tc_channel_order = []
lc_channel_order = []
threads = []
status = {}
csv_files = []
csv_queue = Queue(maxsize=10000)
continuity_queue = Queue(maxsize=500)
countdown_clock = -10
stop_event = threading.Event()
abort_state = threading.Event()
non_abort_shutdown = threading.Event()

conditions_true_affect = {} # dict of for every switch, which switches have it in their conditions_true
conditions_false_affect = {} # dict of for every switch, which switches have it in their conditions_false
for item in master_array: # populates them
    conditions_true_affect[item] = [switch for switch in master_array if item in master_array[switch]['conditions_true']]
    conditions_false_affect[item] = [switch for switch in master_array if item in master_array[switch]['conditions_false']]
conditions_true_affect['abort'] = [switch for switch in master_array if switch != 'abort' and switch not in abort_independent_switches] # Make abort a true condition of all switches except itself and any switches independent from abort state (ex logging)

if operating_mode == 'FGS' or operating_mode == 'miniStand': # Defines which DAQ each ground system uses and prevents having to check all possible values of operating_mode every time LabJack or NI must be used
    daq_system = 'NI'
elif operating_mode == 'flowStand' or operating_mode == 'torchIgniter':
    daq_system = 'LabJack'
    handle = ljm.openS("T7", "ANY", "ANY")  # T7 device, Any connection, Any identifier
    info = ljm.getHandleInfo(handle)
    deviceType = info[0]
else: daq_system = 'Unknown'

error_file = open(f'error_out.csv', 'a', newline='', buffering=1<<15)
error_writer = csv.writer(error_file)
error_writer.writerow(['time','error'])
error_writer.writerow([time.strftime("%Y-%m-%d_%H-%M-%S"),'start'])
error_file.flush()

def write_error(string):
    ts_str = time.strftime("%Y-%m-%d_%H-%M-%S")
    error_writer.writerow([ts_str, string])
    error_file.flush()

# Initialization Functions
def check_user_values():
    # check that all config variables are the correct format within acceptable ranges
    issues = []

    # Check Config Variables
    if type(operating_mode) != str: issues.append(f"TypeError: operating_mode variable '{operating_mode}' is a {type(operating_mode)}. Must be string")
    elif daq_system == 'Unknown': issues.append(f"ValueError: operating_mode variable is '{operating_mode}'. Must be 'FGS', 'miniStand', 'flowStand', or 'torchIngiter'")
    
    if type(do_logging) != bool: issues.append(f"TypeError: do_logging variable '{do_logging}' is a {type(do_logging)}. Must be boolean")
    if do_logging == False: print_out("\nWARNING, WARNING, WARNING\ndo_logging set to False\nNo data will be saved\nWARNING, WARNING, WARNING\n")
    if type(print_switch_changes) != bool: issues.append(f"TypeError: print_switch_changes variable '{print_switch_changes}' is a {type(print_switch_changes)}. Must be boolean")
    if type(save_csv) != bool: issues.append(f"TypeError: save_csv variable '{save_csv}' is a {type(save_csv)}. Must be boolean")
    if save_csv == False: print_out("\nWARNING, WARNING, WARNING\nsave_csv set to False\nNo data will be saved\nWARNING, WARNING, WARNING\n")
    if type(bypass_shift) != bool: issues.append(f"TypeError: bypass_shift variable '{bypass_shift}' is a {type(bypass_shift)}. Must be boolean")
    if bypass_shift == True: print_out("\nWARNING, WARNING, WARNING\nbypass_shift set to True\nNo check for shift being pressed\nWARNING, WARNING, WARNING\n")
    if type(bypass_continuity_check) != bool: issues.append(f"TypeError: bypass_continuity_check variable '{bypass_continuity_check}' is a {type(bypass_continuity_check)}. Must be boolean")
    if bypass_continuity_check == True: print_out("\nWARNING, WARNING, WARNING\nbypass_continuity_check set to True\nNo check for continuity, hotfire will always run\nWARNING, WARNING, WARNING\n")
    global log_output_dir
    if type(log_output_dir) != str: issues.append(f"TypeError: log_output_dir variable '{log_output_dir}' is a {type(log_output_dir)}. Must be string")
    else:
        if log_output_dir == '': log_output_dir = os.path.abspath(os.getcwd()) # If blank, use current working directory
        try:
            os.makedirs(log_output_dir, exist_ok=True)
        except Exception as e:
            _, _, tb = sys.exc_info()
            issues.append(f"{tb.tb_lineno}: 'log_output_dir' DirectoryError: {e}")
    log_output_dir = log_output_dir.rstrip('/') # remove / from the end of path
            
    if type(pt_units) != str: issues.append(f"TypeError: pt_units variable '{pt_units}' is a {type(pt_units)}. Must be string")
    if type(tc_units) != str: issues.append(f"TypeError: tc_units variable '{tc_units}' is a {type(tc_units)}. Must be string")
    if type(lc_units) != str: issues.append(f"TypeError: lc_units variable '{lc_units}' is a {type(lc_units)}. Must be string")
    
    # Check sequence variables
    if type(ox_vent_close_for) != int: issues.append(f"TypeError: ox_vent_close_for variable '{ox_vent_close_for}' is a {type(ox_vent_close_for)}. Must be integer")
    if type(ox_vent_open_for) != int: issues.append(f"TypeError: ox_vent_open_for variable '{ox_vent_open_for}' is a {type(ox_vent_open_for)}. Must be integer")
    
    global mfv_lead_time, hotfire_burn_time, hotfire_purge_time, countdown_step
    if type(mfv_lead_time) != float and type(mfv_lead_time) != int: 
        issues.append(f"TypeError: mfv_lead_time variable '{mfv_lead_time}' is a {type(mfv_lead_time)}. Must be integer or float")
    if type(hotfire_burn_time) != float and type(hotfire_burn_time) != int: 
        issues.append(f"TypeError: hotfire_burn_time variable '{hotfire_burn_time}' is a {type(hotfire_burn_time)}. Must be integer or float")
    if type(hotfire_purge_time) != float and type(hotfire_purge_time) != int: 
        issues.append(f"TypeError: hotfire_purge_time variable '{hotfire_purge_time}' is a {type(hotfire_purge_time)}. Must be integer or float")
    if type(countdown_step) != float and type(countdown_step) != int: 
        issues.append(f"TypeError: countdown_step variable '{countdown_step}' is a {type(countdown_step)}. Must be integer or float")
    
    if daq_system == 'NI': # Check NI config values, even though these variables are needed for the program to run, their values don't matter unless the daq_system is NI
        if type(cDAQ) != int: issues.append(f"TypeError: cDAQ variable '{cDAQ}' is a {type(cDAQ)}. Must be integer")
        
        if type(pt_pull_freq) != int: issues.append(f"TypeError: pt_pull_freq variable '{pt_pull_freq}' is a {type(pt_pull_freq)}. Must be integer")
        if type(pt_push_freq) != int: issues.append(f"TypeError: pt_push_freq variable '{pt_push_freq}' is a {type(pt_push_freq)}. Must be integer")
        if type(tc_pull_freq) != int: issues.append(f"TypeError: tc_pull_freq variable '{tc_pull_freq}' is a {type(tc_pull_freq)}. Must be integer")
        if type(tc_push_freq) != int: issues.append(f"TypeError: tc_push_freq variable '{tc_push_freq}' is a {type(tc_push_freq)}. Must be integer")
        if type(lc_pull_freq) != int: issues.append(f"TypeError: lc_pull_freq variable '{lc_pull_freq}' is a {type(lc_pull_freq)}. Must be integer")
        if type(lc_push_freq) != int: issues.append(f"TypeError: lc_push_freq variable '{lc_push_freq}' is a {type(lc_push_freq)}. Must be integer")
            
        # Check modules and channels are valid
        system = nidaqmx.system.System.local() # Instatiates the top level NI-DAQmx object, allowing us to interface with the hardware
        ssr_modules = []
        pt_modules = []
        tc_modules = []
        lc_modules = []
        if str(cDAQ) not in [device.name[device.name.find('cDAQ')+4:] for device in system.devices if len(device.name) < 9]:
            issues.append(f"ValueError: cDAQ '{cDAQ}' not a valid cDAQ chassis")
        else:
            for device in system.devices:
                device_cdaq = device.name[device.name.find('cDAQ')+4:device.name.find('Mod')]
                if device_cdaq == str(cDAQ):
                    if "9205" in device.product_type: # PT Voltage
                        pt_modules.append((int(device.name[device.name.find('Mod')+3:])-1,32))
                    elif "9213" in device.product_type: # TC
                        tc_modules.append((int(device.name[device.name.find('Mod')+3:])-1,8))
                    elif "9237" in device.product_type: # LC
                        lc_modules.append((int(device.name[device.name.find('Mod')+3:])-1,2))
                    elif "9253" in device.product_type: # PT Current
                        pt_modules.append((int(device.name[device.name.find('Mod')+3:])-1,8))
                    elif "9401" in device.product_type: # Unused
                        pass
                    elif "9485" in device.product_type: # SSR
                        ssr_modules.append((int(device.name[device.name.find('Mod')+3:])-1,8))
            
            # Only running the following checks if valid cDAQ because otherwise they would all cause errors because the modules for each sensor could not have been found
            all_sensor_errors = []
            all_valve_errors= []
            for sensor in sensor_array:
                sensor_errors = []
                modNum = sensor_array[sensor]['module']
                chanNum = sensor_array[sensor]['channel']
                sensor_type = sensor_array[sensor]['sensor_type']
                if sensor_type == 'PT': modules = pt_modules
                elif sensor_type == 'TC': modules = tc_modules
                elif sensor_type == 'LC': modules = lc_modules
                modNums = [mod[0] for mod in modules]
                
                # Check module exists
                mod = sensor_array[sensor]['module']
                if modNum not in modNums:
                    sensor_errors.append(f"Module {mod} not valid for sensor_type {sensor_type}")
                else:
                    module_lookup = {}
                    for mod in modules:
                        module_lookup[mod[0]] = mod[1]
                    if not (0 <= chanNum < module_lookup[modNum]):
                        sensor_errors.append(f"Channel {chanNum} out of module {modNum}'s channel range, 0-{module_lookup[modNum]-1}")

                if sensor_errors:
                    all_sensor_errors.append(f"{sensor} is invalid: {', '.join(sensor_errors)}")
                    
            for valve in valve_array:
                valve_errors = []
                if valve_array[valve]['module'] not in [mod[0] for mod in ssr_modules]:
                    valve_errors.append(f'Module {valve_array[valve]['module']} not valid for SSR Relays')
                else:
                    if not (0 <= valve_array[valve]['channel'] < 8):
                        valve_errors.append(f"Channel {valve_array[valve]['channel']} out of channel range 0-7")
                    
                if valve_errors:
                    all_valve_errors.append(f"{valve} is invalid: {', '.join(valve_errors)}")
                    
            if all_sensor_errors:
                issues.append(f"Invalid sensors:\n{'\n'.join(all_sensor_errors)}")
            if all_valve_errors:
                issues.append(f"Invalid valves:\n{'\n'.join(all_valve_errors)}")
            
        # Check that no sensors or valves are trying to use the same module and channel
        sensor_dupes = {
            (v["module"], v["channel"]): [k2 for k2, v2 in sensor_array.items()
                                        if v2["module"] == v["module"] and v2["channel"] == v["channel"]]
            for k, v in sensor_array.items()
            if sum(v2["module"] == v["module"] and v2["channel"] == v["channel"] for v2 in sensor_array.values()) > 1
        }
        valve_dupes = {
            (v["module"], v["channel"]): [k2 for k2, v2 in valve_array.items()
                                        if v2["module"] == v["module"] and v2["channel"] == v["channel"]]
            for k, v in valve_array.items()
            if sum(v2["module"] == v["module"] and v2["channel"] == v["channel"] for v2 in valve_array.values()) > 1
        }
        if sensor_dupes or valve_dupes:
            message = ''
            if sensor_dupes:
                message = message + f'Multiple sensors on the same NI module and channel: {sensor_dupes}\n'
            if valve_dupes:
                message = message + f'Multiple valves on the same NI module and channel: {valve_dupes}\n'
            issues.append(f"IndexError: {message.strip()}")
            
    elif daq_system == 'LabJack': # Check LabJack config values, even though these variables are needed for the program to run, their values don't matter unless the daq_system is NI
        
        if type(sensor_pull_freq) != int: issues.append(f"TypeError: sensor_pull_freq variable '{sensor_pull_freq}' is a {type(sensor_pull_freq)}. Must be integer")
        if type(sensor_push_freq) != int: issues.append(f"TypeError: sensor_push_freq variable '{sensor_push_freq}' is a {type(sensor_push_freq)}. Must be integer")
        
        # Check that all valves and switches are using appropriate channels
        sensor_errors = []
        valve_errors= []
        acceptable_sensor_channels = ['AIN0', 'AIN1', 'AIN2', 'AIN3', 'AIN4', 'AIN5', 'AIN6', 'AIN7', 'AIN8', 'AIN9', 'AIN10', 'AIN11', 'AIN12', 'AIN13']
        acceptable_valve_channels = ['FIO0', 'FIO1', 'FIO2', 'FIO3', 'FIO4', 'FIO5', 'FIO6', 'FIO7']
        for sensor in sensor_array:
            channel = sensor_array[sensor]['channel']
            if channel not in acceptable_sensor_channels:
                sensor_errors.append(f"Channel {channel} not acceptable channel for sensor")
                
        for valve in valve_array:
            valve_errors = []
            channel = valve_array[valve]['channel']
            if channel not in acceptable_valve_channels:
                valve_errors.append(f"Channel {channel} not acceptable channel for valve")
                
        if sensor_errors:
            issues.append(f"Invalid sensors:\n{'\n'.join(all_sensor_errors)}\n\n")
        if valve_errors:
            issues.append(f"Invalid valves:\n{'\n'.join(all_valve_errors)}")
        
        # Check that no valve or sensor is trying to use the same channel as another
        labjack_channels = valve_array | sensor_array
        channels = {} # Check for duplicate channels
        for name, info in labjack_channels.items():
            ch = info.get('channel')
            channels.setdefault(ch, []).append(name)

        duplicates = {ch: names for ch, names in channels.items() if len(names) > 1}
        
        if duplicates:
            issues.append(f"IndexError: Multiple sensors on the same NI module and channel: {duplicates}\n")

    # Check for proper dictionary formats of sensor_array, valve_array, lockout_array, and sequence_array
    for sensor in sensor_array:
        if 'sensor_type' in sensor_array[sensor]: 
            if type(sensor_array[sensor]['sensor_type']) != str: 
                issues.append(f"TypeError: {sensor} in sensor_array attribute sensor_type is a {type(sensor_array[sensor]['sensor_type'])}. Must be string")
        else: 
            issues.append(f"AttributeError: {sensor} in sensor_array is missing attribute 'sensor_type'")
        
        if daq_system == 'NI':
            if 'module' in sensor_array[sensor]: 
                if type(sensor_array[sensor]['module']) != int: 
                    issues.append(f"TypeError: {sensor} in sensor_array attribute module is a {type(sensor_array[sensor]['module'])}. Must be integer")
            else: 
                issues.append(f"AttributeError: {sensor} in valve_array is missing attribute 'module'")
        
        if 'channel' in sensor_array[sensor]: 
            if daq_system == 'NI':
                if type(sensor_array[sensor]['channel']) != int: 
                    issues.append(f"TypeError: {sensor} in sensor_array attribute channel is a {type(sensor_array[sensor]['channel'])}. Must be integer")
            elif daq_system == 'LabJack':
                if type(sensor_array[sensor]['channel']) != str: 
                    issues.append(f"TypeError: {sensor} in sensor_array attribute channel is a {type(sensor_array[sensor]['channel'])}. Must be string")
        else: 
            issues.append(f"AttributeError: {valve} in valve_array is missing attribute 'channel'")
        
        if 'multiplier' in sensor_array[sensor]: 
            if type(sensor_array[sensor]['multiplier']) != int and type(sensor_array[sensor]['multiplier']) != float: 
                issues.append(f"TypeError: {sensor} in sensor_array attribute multiplier is a {type(sensor_array[sensor]['multiplier'])}. Must be integer or float")
        
        if 'offset' in sensor_array[sensor]: 
            if type(sensor_array[sensor]['offset']) != int and type(sensor_array[sensor]['offset']) != float: 
                issues.append(f"TypeError: {sensor} in sensor_array attribute offset is a {type(sensor_array[sensor]['offset'])}. Must be integer or float")
    
    for valve in valve_array: 
        if 'state' in valve_array[valve]: 
            if type(valve_array[valve]['state']) != bool: 
                issues.append(f"TypeError: {valve} in valve_array attribute state is a {type(valve_array[valve]['state'])}. Must be boolean")
        else: 
            issues.append(f"AttributeError: {valve} in valve_array is missing attribute 'state'")
        
        if daq_system == 'NI':
            if 'module' in valve_array[valve]: 
                if type(valve_array[valve]['module']) != int: 
                    issues.append(f"TypeError: {valve} in valve_array attribute module is a {type(valve_array[valve]['module'])}. Must be integer")
            else: 
                issues.append(f"AttributeError: {valve} in valve_array is missing attribute 'module'")
        
        if 'channel' in valve_array[valve]: 
            if daq_system == 'NI':
                if type(valve_array[valve]['channel']) != int: 
                    issues.append(f"TypeError: {valve} in valve_array attribute channel is a {type(valve_array[valve]['channel'])}. Must be integer")
            elif daq_system == 'LabJack':
                if type(valve_array[valve]['channel']) != str: 
                    issues.append(f"TypeError: {valve} in valve_array attribute channel is a {type(valve_array[valve]['channel'])}. Must be string")
        else: 
            issues.append(f"AttributeError: {valve} in valve_array is missing attribute 'channel'")
        
        if 'conditions_true' in valve_array[valve]: 
            if type(valve_array[valve]['conditions_true']) != list: 
                issues.append(f"TypeError: {valve} in valve_array attribute 'conditions_true' is a {type(valve_array[valve]['conditions_true'])}. Must be list")
            else:
                for item in valve_array[valve]['conditions_true']:
                    if item not in master_array:
                        issues.append(f"KeyError: {item} in 'conditions_true' in {valve} in valve_array attribute is not a valid switch")
        else: 
            issues.append(f"AttributeError: {valve} in valve_array is missing attribute 'conditions_true'")
        
        if 'conditions_false' in valve_array[valve]: 
            if type(valve_array[valve]['conditions_false']) != list: 
                issues.append(f"TypeError: {valve} in valve_array attribute 'conditions_false' is a {type(valve_array[valve]['conditions_false'])}. Must be list")
            else:
                for item in valve_array[valve]['conditions_false']:
                    if item not in master_array:
                        issues.append(f"KeyError: {item} in 'conditions_false' in {valve} in valve_array attribute is not a valid switch")
        else: 
            issues.append(f"AttributeError: {valve} in valve_array is missing attribute 'conditions_false'")
        
        if 'plain_name' in valve_array[valve]: 
            if type(valve_array[valve]['plain_name']) != str: 
                issues.append(f"TypeError: {valve} in valve_array attribute plain_name is a {type(valve_array[valve]['plain_name'])}. Must be string")
        else: 
            issues.append(f"AttributeError: {valve} in valve_array is missing attribute 'plain_name'")
        
    for cond in lockout_array: 
        if 'state' in lockout_array[cond]: 
            if type(lockout_array[cond]['state']) != bool: 
                issues.append(f"TypeError: {cond} in lockout_array attribute state is a {type(lockout_array[cond]['state'])}. Must be boolean")
        else: 
            issues.append(f"AttributeError: {cond} in lockout_array is missing attribute 'state'")
        
        if 'conditions_true' in lockout_array[cond]: 
            if type(lockout_array[cond]['conditions_true']) != list: 
                issues.append(f"TypeError: {cond} in lockout_array attribute 'conditions_true' is a {type(lockout_array[cond]['conditions_true'])}. Must be list")
            else:
                for item in lockout_array[cond]['conditions_true']:
                    if item not in master_array:
                        issues.append(f"KeyError: {item} in 'conditions_true' in {cond} in lockout_array attribute is not a valid switch")
        else: 
            issues.append(f"AttributeError: {cond} in lockout_array is missing attribute 'conditions_true'")
        
        if 'conditions_false' in lockout_array[cond]: 
            if type(lockout_array[cond]['conditions_false']) != list: 
                issues.append(f"TypeError: {cond} in lockout_array attribute 'conditions_false' is a {type(lockout_array[cond]['conditions_false'])}. Must be list")
            else:
                for item in lockout_array[cond]['conditions_false']:
                    if item not in master_array:
                        issues.append(f"KeyError: {item} in 'conditions_false' in {cond} in lockout_array attribute is not a valid switch")
        else: 
            issues.append(f"AttributeError: {cond} in lockout_array is missing attribute 'conditions_false'")
        
        if 'plain_name' in lockout_array[cond]: 
            if type(lockout_array[cond]['plain_name']) != str: 
                issues.append(f"TypeError: {cond} in lockout_array attribute plain_name is a {type(lockout_array[cond]['plain_name'])}. Must be string")
        else: 
            issues.append(f"AttributeError: {cond} in lockout_array is missing attribute 'plain_name'")
        
    for seq in sequence_array: 
        if 'state' in sequence_array[seq]: 
            if type(sequence_array[seq]['state']) != bool: 
                issues.append(f"TypeError: {seq} in sequence_array attribute state is a {type(sequence_array[seq]['state'])}. Must be boolean")
        else: 
            issues.append(f"AttributeError: {seq} in sequence_array is missing attribute 'state'")
        
        if 'conditions_true' in sequence_array[seq]: 
            if type(sequence_array[seq]['conditions_true']) != list: 
                issues.append(f"TypeError: {seq} in sequence_array attribute 'conditions_true' is a {type(sequence_array[seq]['conditions_true'])}. Must be list")
            else:
                for item in sequence_array[seq]['conditions_true']:
                    if item not in master_array:
                        issues.append(f"KeyError: {item} in 'conditions_true' in {seq} in sequence_array attribute is not a valid switch")
        else: 
            issues.append(f"AttributeError: {seq} in sequence_array is missing attribute 'conditions_true'")
        
        if 'conditions_false' in sequence_array[seq]: 
            if type(sequence_array[seq]['conditions_false']) != list: 
                issues.append(f"TypeError: {seq} in sequence_array attribute 'conditions_false' is a {type(sequence_array[seq]['conditions_false'])}. Must be list")
            else:
                for item in sequence_array[seq]['conditions_false']:
                    if item not in master_array:
                        issues.append(f"KeyError: {item} in 'conditions_false' in {seq} in sequence_array attribute is not a valid switch")
        else: 
            issues.append(f"AttributeError: {seq} in sequence_array is missing attribute 'conditions_false'")
        
        if 'function' in sequence_array[seq]: 
            if type(sequence_array[seq]['function']) != type(init_ni): 
                issues.append(f"TypeError: {seq} in sequence_array attribute function is a {type(sequence_array[seq]['function'])}. Must be a function")
        else: 
            issues.append(f"AttributeError: {seq} in sequence_array is missing attribute 'function'")
        
        if 'thread' in sequence_array[seq]: 
            if type(sequence_array[seq]['thread']) != bool: 
                issues.append(f"TypeError: {seq} in sequence_array attribute thread is a {type(sequence_array[seq]['thread'])}. Must be boolean")
        else: 
            issues.append(f"AttributeError: {seq} in sequence_array is missing attribute 'thread'")
        
        if 'plain_name' in sequence_array[seq]: 
            if type(sequence_array[seq]['plain_name']) != str: 
                issues.append(f"TypeError: {seq} in sequence_array attribute plain_name is a {type(sequence_array[seq]['plain_name'])}. Must be string")
        else: 
            issues.append(f"AttributeError: {seq} in sequence_array is missing attribute 'plain_name'")
    
    # Check that all sequence function_names are unique in sequence_array
    function_name_groups = {}
    for seq, info in sequence_array.items():
        fn = info["function_name"]
        function_name_groups.setdefault(fn, []).append(seq)
    duplicates = {func: seqs for func, seqs in function_name_groups.items() if len(seqs) > 1}
    if duplicates:
        issues.append(f"IndexError: Multiple sequences use the same function_names in sequence_array, must be unique: {duplicates}")
             
    # Check that all sequence functions are unique in function_name_interpreter
    function_groups = {}
    for function_name, func in function_name_interpreter.items():
        function_groups.setdefault(func, []).append(function_name)
    duplicates = {func.__name__: function_names for func, function_names in function_groups.items() if len(function_names) > 1}
    if duplicates:
        issues.append(f"IndexError: Multiple function_names use the same function in function_name_interpreter, must be unique: {duplicates}")
             
    # Check that no conditions cause conflicts
    list_of_issues = []
    for switch in master_array:
        conditions_true = master_array[switch]['conditions_true']
        conditions_false = master_array[switch]['conditions_false']
        all_conditions = conditions_true + conditions_false
        
        # Check for self in conditions
        if switch in conditions_true: 
            issues.append(f"KeyError: {switch} in own conditions_true")
            list_of_issues.append((switch))
        if switch in conditions_false: 
            issues.append(f"KeyError: {switch} in own conditions_false")
            list_of_issues.append((switch))
            
        # Check for duplicate conditions
        if len(conditions_true) != len(set(conditions_true)): 
            issues.append(f'KeyError: {switch} has duplicate conditions {', '.join([cond for cond in set(conditions_true) if conditions_true.count(cond) > 1])} in conditions_true')
        if len(conditions_false) != len(set(conditions_false)): 
            issues.append(f'KeyError: {switch} has duplicate conditions {', '.join([cond for cond in set(conditions_false) if conditions_false.count(cond) > 1])} in conditions_false')
        
        # Check for same condition in both conditions_true and conditions_false
        for cond in set(all_conditions):
            if cond != switch: # Ignore self in conditions
                if cond in conditions_true and cond in conditions_false: 
                    issues.append(f'KeyError: {switch} has condition {cond} in both conditions_true and conditions_false')
                    
        # Check for mutually true (ie 2 switches have each other in conditions_true, thus impossible to actuate)
        for cond in conditions_true:
            if cond != switch and not any({switch,cond} <= set(t) for t in list_of_issues): # Ignore self in conditions and already detected issues
                if switch in master_array[cond]['conditions_true']:
                    issues.append(f"KeyError: {switch} and {cond} both have each other in conditions_true")
                    list_of_issues.append((switch, cond))
                
        # Protect mutually exclusive switches (ie 2 switches have each other in conditions_false, thus making only one possible to be true at a time)
        for cond in conditions_false:
            if cond != switch and not any({switch,cond} <= set(t) for t in list_of_issues): # Ignore self in conditions and already detected issues
                if switch in master_array[cond]['conditions_false']:
                    list_of_issues.append((switch, cond))
    
                
    # Check for self shutoff from conditions (ie a chain of conditions that would cause a single switch to cause its own conditions to prevent its actuations)
    graph = {}
    for switch, data in master_array.items():
        graph[switch] = [(c, True) for c in data.get("conditions_true", [])] + [(c, False) for c in data.get("conditions_false", [])]

    visited = set()
    path = []

    def dfs(switch):
        if switch in path:
            cycle = path[path.index(switch):] + [switch]
            # Build a readable representation of dependency types in the cycle
            arrows = []
            for i in range(len(cycle) - 1):
                source = cycle[i]
                destination = cycle[i + 1]
                # find polarity of this edge
                for (c, polarity) in graph[source]:
                    if c == destination:
                        arrows.append(f"{source} needs {destination} {'True' if polarity else 'False'}")
                        break
                    
            if not any({switch,c} <= set(t) for t in list_of_issues):
                issues.append(f"KeyError: {switch} deactuates itself: " + " -> ".join(arrows))
            return
        if switch in visited:
            return
        visited.add(switch)
        path.append(switch)
        for next, _ in graph.get(switch, []):
            if next in master_array:  # skip unknowns already logged
                dfs(next)
        path.pop()

    for switch in master_array:
        dfs(switch)
        
    # Raise issues
    print_out('')
    if issues != []:
        error_out(Exception("\n"+"\n\n".join(issues)))
        
def init_ni(): # Build all data needed for NI operations and initialize NI devices
    system = nidaqmx.system.System.local() # Instatiates the top level NI-DAQmx object, allowing us to interface with the hardware
    ssr_modules = []
    pt_modules = []
    tc_modules = []
    lc_modules = []
    
    # Initialize a task for each sensor type
    ni_reader_tasks = [None, None, None]
    for i in range(len(ni_reader_tasks)):
        task = nidaqmx.Task()
        ni_tasks.append(task)
        ni_reader_tasks[i] = task
        
    for device in system.devices: # Add modules to tasks and arrays in correct order (for data processing). 
        device_cdaq = device.name[device.name.find('cDAQ')+4:device.name.find('Mod')]
        if device_cdaq == str(cDAQ): 
            if "9189" in device.product_type: #cDAQ Chassis
                pass
            elif "9205" in device.product_type: # Voltage (PT)
                #print_out(f"NI-9205 Analog Voltage Input Module {device.name} Connected")
                pt_modules.append((int(device.name[device.name.find('Mod')+3:])-1, 32, 'PT'))
                try:
                    ni_reader_tasks[0].ai_channels.add_ai_voltage_chan(
                        f'{device.name}/ai0:31',
                        terminal_config=TerminalConfiguration.RSE,
                        min_val=-10, max_val=10
                    )
                except Exception as e:
                    _, _, tb = sys.exc_info()
                    error_out(ConnectionError(f"NI Initialization Error: Failed to add PT voltage channels {device.name}: {type(e).__name__} on line {tb.tb_lineno}: {e}"))

            elif "9253" in device.product_type: # Current (PT)
                #print_out(f"NI-9253 Analog Current Input Module {device.name} Connected")
                pt_modules.append((int(device.name[device.name.find('Mod')+3:])-1, 8, 'PT'))
                try:
                    ni_reader_tasks[0].ai_channels.add_ai_current_chan(
                        f'{device.name}/ai0:7',
                        terminal_config=TerminalConfiguration.RSE,
                        min_val=-0.02, max_val=0.02, # ±20 mA range
                        units=nidaqmx.constants.CurrentUnits.AMPS
                    )
                except Exception as e:
                    _, _, tb = sys.exc_info()
                    error_out(ConnectionError(f"NI Initialization Error: Failed to add PT current channels {device.name}: {type(e).__name__} on line {tb.tb_lineno}: {e}"))

            elif "9213" in device.product_type: # TC
                #print_out(f"NI-9213 Thermocouple Module {device.name} Connected")
                tc_modules.append((int(device.name[device.name.find('Mod')+3:])-1, 8, 'TC'))
                try:
                    ni_reader_tasks[1].ai_channels.add_ai_thrmcpl_chan(
                    physical_channel=f'{device.name}/ai0:7',
                    min_val=-200, max_val=1260,
                    thermocouple_type=nidaqmx.constants.ThermocoupleType.K,
                    cjc_source=nidaqmx.constants.CJCSource.BUILT_IN
                    )
                except Exception as e:
                    _, _, tb = sys.exc_info()
                    error_out(ConnectionError(f"NI Initialization Error: Failed to add channels {device.name}: {type(e).__name__} on line {tb.tb_lineno}: {e}"))

            elif "9237" in device.product_type: # LC
                #print_out(f"NI-9237 Load Cell Module {device.name} Connected")
                lc_modules.append((int(device.name[device.name.find('Mod')+3:])-1, 2, 'LC'))
                try:
                    ni_reader_tasks[2].ai_channels.add_ai_force_bridge_table_chan(
                    f"{device.name}/ai0:1",
                    min_val=0,
                    max_val=2000,
                    voltage_excit_val=10,
                    nominal_bridge_resistance=700,
                    electrical_vals=[0, -0.3710, -0.7418, -1.0200, -1.3909, -1.8547],
                    physical_vals=[0, 400, 800, 1100, 1500, 2000]
                    )
                except Exception as e:
                    _, _, tb = sys.exc_info()
                    error_out(ConnectionError(f"NI Initialization Error: Failed to add LC channels {device.name}: {type(e).__name__} on line {tb.tb_lineno}: {e}"))
                
            elif "9485" in device.product_type: # SSR
                #print_out(f"NI-9485 SSR Module {device.name} Connected")
                ssr_modules.append(device.name)
                 
            elif "9401" in device.product_type: # Legacy. Unused
                #print_out(f"NI-9401 Digital I/O Module {device.name} Skipped. This device is not used by this program.")
                pass
                
            else:
                #print_out(f"Unknown NI Module {device.product_type} {device.name}. Ignoring")
                pass
                
        else: 
            if "9189" in device.product_type:
                #print_out(f"Skipping {device.name} {device.product_type}. Chassis not selected")
                pass
    
    global ssr_mods
    global ssr_writers
    ssr_mods = [None] * len(ssr_modules) # Configuring for write_single_switch
    ssr_writers = [None] * len(ssr_modules) # Configuring for write_single_switch
    for i in range(len(ssr_modules)): # Initialize SSR modules
        device = ssr_modules[i]
        task = nidaqmx.Task()
        writer = DigitalMultiChannelWriter(task.out_stream)
        for line in range(8):
            task.do_channels.add_do_chan(f"{device}/port0/line{line}")
        writer.write_one_sample_one_line(np.array([False]*8)) # Write all channels nominal at start
        task.start()
        modNum = int(device[-1])-1 # -1 for NI counting correction
    
        ni_tasks.append(task)
        ssr_mods[i] = modNum
        ssr_writers[i] = writer
        
    for switch in valve_array: # Add ssr_mod for write_single_switch
        for i in range(len(ssr_modules)):
            if valve_array[switch]['module'] == ssr_mods[i]:
                valve_array[switch]['ssr_mod'] = i
                master_array[switch]['ssr_mod'] = i
        
    return pt_modules, tc_modules, lc_modules, ni_reader_tasks, ssr_mods


# Synnax Functions
def build_sensor_data(sensor_type, make_channels=True):
    try:
        # Define the order of the channels for each sensor for data handling
        if sensor_type == 'PT': channel_order = pt_channel_order
        elif sensor_type == 'TC': channel_order = tc_channel_order
        elif sensor_type == 'LC': channel_order = lc_channel_order
        
        channel_names = [None] * len(channel_order)
        scaling_factors = [None] * len(channel_order)
        
        if daq_system == 'NI':
            lookup = { # Get name from module and channel
                (v["module"], v["channel"]): {"name": k, **{kk: vv for kk, vv in v.items()}}
                for k, v in sensor_array.items()
            }
            
            for channel_number in range(len(channel_order)):
                channel=channel_order[channel_number]
                modNum = int(channel[channel.find('Mod')+3:channel.find('/ai')]) -1 # Get module number from NI channel
                chanNum = int(channel[channel.find('/ai')+3:]) # Get channel number from NI channel
                if (modNum, chanNum) in lookup: # All NI channels are initialized in the NI task, but there are not always sensors for those channels
                    row = lookup[(modNum, chanNum)]
                    name = row['name']
                    channel_names[channel_number] = f'mod{modNum}_chan{chanNum}_{name}' # pretty channel name in same order as channel_order
                    if 'multiplier' in row or 'offset' in row: # If this sensor has scaling
                        multiplier = 1 # Default
                        offset = 0 # Default
                        if 'multiplier' in row:
                            multiplier = row['multiplier']
                        if 'offset' in row:
                            offset = row['offset']
                        scaling_factors[channel_number] = (multiplier, offset) # will be left as None unless it has multiplier or offset
            
        elif daq_system == 'LabJack':
            lookup = { # Get name from channel
                v["channel"]: {"name": k, **{kk: vv for kk, vv in v.items()}}
                for k, v in sensor_array.items()
            }
            for channel_number in range(len(channel_order)):
                channel = channel_order[channel_number]
                row = lookup[channel] 
                name = row['name'] 
                channel_names[channel_number] = f'chan{channel}_{name}'
                if 'multiplier' in row or 'offset' in row: # If this sensor has scaling
                    multiplier = 1 # Default
                    offset = 0 # Default
                    if 'multiplier' in row:
                        multiplier = row['multiplier']
                    if 'offset' in row:
                        offset = row['offset']
                    scaling_factors[channel_number] = (multiplier, offset) # will be left as None unless it has multiplier or offset
        
        # Make empty variables
        time_channel = None 
        scaled_channels = [None] * len(channel_order)
        raw_channels = [None] * len(channel_order)
        
        if make_channels: # Only actually return the channels if called (not necessary, but left for efficiency)
            if sensor_type == 'PT': units = pt_units
            elif sensor_type == 'TC': units = tc_units
            elif sensor_type == 'LC': units = lc_units
                
            time_channel = client.channels.create(
                name=f"{sensor_type}_time_channel",
                is_index=True,
                data_type=sy.DataType.TIMESTAMP,
                retrieve_if_name_exists=True,
            )
            
            for i in range(len(channel_order)): # make channels with pretty names
                if channel_names[i] != None:
                    scaled_channel = client.channels.create(
                        name=f"{sensor_type}_{units}_{channel_names[i]}",
                        index=time_channel.key,
                        data_type=sy.DataType.FLOAT32,
                        retrieve_if_name_exists=True,
                    )
                    scaled_channels.append(scaled_channel)
                        
                    if scaling_factors[i] != None: # only make raw channel if there is scaling for that sensor
                        raw_channel = client.channels.create(
                            name=f"{sensor_type}_raw_{channel_names[i]}",
                            index=time_channel.key,
                            data_type=sy.DataType.FLOAT32,
                            retrieve_if_name_exists=True,
                        )
                        raw_channels.append(raw_channel)
                
        return scaling_factors, channel_names, time_channel, raw_channels, scaled_channels
    
    except Exception as e:
        _, _, tb = sys.exc_info()
        error_out(Exception(f"Failed to create synnax sensor channels: {type(e).__name__} on line {tb.tb_lineno}: {e}"))
  
def create_synnax_switch_channels(channel_name):
    try:
        state_time = client.channels.create(
            name=f"{channel_name}_state_time",
            is_index=True,
            data_type=sy.DataType.TIMESTAMP,
            retrieve_if_name_exists=True
        )
        cmd_time = client.channels.create(
            name=f"{channel_name}_cmd_time",
            is_index=True,
            data_type=sy.DataType.TIMESTAMP,
            retrieve_if_name_exists=True
        )
        state = client.channels.create(
            name=f"{channel_name}_state",
            data_type=sy.DataType.UINT8,
            index=state_time.key,
            retrieve_if_name_exists=True
        )
        cmd = client.channels.create(
            name=f"{channel_name}_cmd",
            data_type=sy.DataType.UINT8,
            index=cmd_time.key,
            retrieve_if_name_exists=True
        )
        return state.name, cmd.name, state_time.name, cmd_time.name
    except Exception as e:
        _, _, tb = sys.exc_info()
        error_out(Exception(f"Failed to create synnax switch channel: {type(e).__name__} on line {tb.tb_lineno}: {e}"))

def get_safe_start_time(master_time_channel, thread_name):
    try:
        current_time = sy.TimeStamp.now()
        current_datetime = current_time.datetime()
        current_datetime = current_datetime.replace(tzinfo=None)
        base_timestamp = current_time
        # the following tests should never be necessary, but in a previous version a synnax time channel was mishandled and required this safety check, left in for robustness
        try: # master_time is constantly being written to by the main thread and when valve and data threads close. This just makes sure that we are in the future compared to that time
            retrieved_time = client.read_latest(master_time_channel.name)
            retrieved_datetime = datetime.fromtimestamp(retrieved_time[0] // 1000000000)
        except IndexError:
            retrieved_datetime = current_datetime
        except Exception as e: # No existing time data
            _, _, tb = sys.exc_info()
            error_out(Exception(f"Failed to get safe start time for {thread_name} Synnax Writer. Error retrieving time data: {type(e).__name__} on line {tb.tb_lineno}: {e}"))
        time_dif = retrieved_datetime - current_datetime
        if current_datetime < retrieved_datetime and abs(time_dif.total_seconds()) < 5: # if retrieved time in near future, add a few secs
            base_timestamp = current_time + sy.TimeSpan.SECOND * 5
            time.sleep(5) # wait to catch up with synnax time
        elif current_datetime < retrieved_datetime and abs(time_dif.total_seconds()) >= 5: # if retrieved time is in the far future
            error_out(IndexError(f'Retrieved date from master_time_channel is {retrieved_time} and is {abs(time_dif.total_seconds())} seconds in the future. Resolve this paradox'))
        return base_timestamp
    except Exception as e:
        _, _, tb = sys.exc_info()
        error_out(Exception(f"Failed to connect {thread_name} Synnax Writer with Synnax or failed to get safe timestamp: {type(e).__name__} on line {tb.tb_lineno}: {e}"))

def write_single_switch(writer, item, value, verbose=True):
    try: # item is the switch to be actuated
        if not stop_event.is_set():
            synnax_state = {}
            
            inhibited = any(not bool(status[cond]['value']) for cond in master_array[item]['conditions_true']) or any(status[cond]['value'] for cond in master_array[item]['conditions_false'])
            if inhibited: value = 0
                        
            timestamp = sy.TimeStamp.now()
            status[item]['value'] = value
            synnax_state[status[item]['state']] = status[item]['value']
            synnax_state[status[item]['state_time']] = timestamp
            
            if print_switch_changes and verbose: # Print State change
                if not inhibited:
                    if item in valve_array: # Define valve_state
                        if bool(value):
                            if valve_array[item]['nominal'] == 'Closed': valve_state = 'Open'
                            elif valve_array[item]['nominal'] == 'Open': valve_state = 'Closed'
                            elif valve_array[item]['nominal'] == 'On': valve_state = 'Off'
                            else: valve_state = 'On' # Good default
                        else:
                            valve_state = valve_array[item]['nominal']
                        print_out(f'{valve_array[item]['plain_name']} set to {valve_state}')
                    else:
                        print_out(f'{master_array[item]['plain_name']} set to {'On' if bool(value) else 'Off'}')
                else:
                    blocked = [master_array[condition]['plain_name'] for condition in master_array[item]['conditions_true'] if bool(status[condition]['value']) == False] + [master_array[condition]['plain_name'] for condition in master_array[item]['conditions_false'] if bool(status[condition]['value']) == True]  # A list of all inhbiiting conditions
                    
                    if item in valve_array: # Use nominal
                        print_out(f'{master_array[item]['plain_name']} set to {valve_array[item]['nominal']}. Inhibited by {blocked}')
                    else:
                        print_out(f'{master_array[item]['plain_name']} set to Off. Inhibited by {blocked}')
                    
                    
            if bool(value): 
                cond_array = conditions_false_affect
            else: 
                cond_array = conditions_true_affect
            turning_off = [cond for cond in cond_array[item] if bool(status[cond]['value']) == True] # list of all switches to turn off with this switches actuation
            prop_complete = False
            while not prop_complete: # this populates the turning_off array with all the switches to be turned off by the turning off of all the switches already in turning_off
                start = turning_off
                for switch in turning_off:
                    turning_off = turning_off + [cond for cond in conditions_true_affect[switch] if cond not in turning_off and bool(status[cond]['value']) == True]
                if turning_off == start: prop_complete = True
            
            if turning_off:
                if 'hotfire' in turning_off: # Exception for aborting and closing hotfires, main loop takes care of this, hotfire does not go forever and will close itself
                    turning_off.remove('hotfire') # doing this to keep hotfire in 
                    status['hotfire']['value'] = 0
                    synnax_state[status['hotfire']['state']] = status['hotfire']['value']
                    synnax_state[status['hotfire']['state_time']] = timestamp
                    status[switch]['event'].set()
                print_out(f'{master_array[item]['plain_name']} turning off {turning_off}')
                for switch in turning_off: # Turn off the switches
                    status[switch]['value'] = 0
                    synnax_state[status[switch]['state']] = status[switch]['value']
                    synnax_state[status[switch]['state_time']] = timestamp
                    if switch in sequence_array: # Stop sequence threads
                        if sequence_array[switch]['thread'] == True:
                            if status[switch]['thread'] != None:
                                status[switch]['event'].set()
                                status[switch]['thread'].join()
                                threads.remove((f'{sequence_array[switch]['plain_name']} Thread', status[switch]['thread']))
                                status[switch]['thread'] = None
                        else: # Stop non thread sequences
                            sequence_array[switch]['function'](writer)
                         
            newTimestamp = sy.TimeStamp.now() # Resetting the timestamps because closing the sequences can write a more recent timestamp on some channels (ex dual_mpvas) and cause issues with the synnax writer
            for chan in synnax_state:
                if synnax_state[chan] == timestamp:
                    synnax_state[chan] = newTimestamp
                             
            if inhibited: # If inhibited, BREIFLY writes to synnax that the switch changed to reset the cmd channel for the next attempt to actuate the switch. Otherwise the inhibited actuation will remain in the synnax frame and will want to actuate and will cause issues
                writer.write({status[item]['state']:1,status[item]['state_time']: timestamp})
                
            if not abort_state.is_set(): # Writes valve states to NI or LabJack
                if daq_system == 'NI':
                    valve_states = [[False,False,False,False,False,False,False,False]] * len(ssr_mods)
                    for switch in valve_array:
                        valve_states[valve_array[switch]['ssr_mod']][valve_array[switch]['channel']] = bool(status[switch]['value'])
                    for i in range(len(ssr_writers)):
                        ssr_writers[i].write_one_sample_one_line(np.array(valve_states[i]))
                elif daq_system == 'LabJack':
                    for switch in valve_array:
                        ljm.eWriteName(handle, valve_array[switch]['channel'], status[switch]['value']) # Writing in a loop to avoid needing to deal with aligning channels and values to the same position in arrays (see how NI is handled)
                        
            writer.write(synnax_state)
            
            if item in sequence_array and not inhibited: # Run sequence if switch is a sequence. At end so the synnax switch changes without needing to wait for the sequence to complete
                if sequence_array[item]['thread'] == True:
                    if bool(value): # Start thread
                        status[item]['event'].clear()
                        thread = threading.Thread(
                            target=sequence_array[item]['function'],
                            args=[writer],
                            name=f'{sequence_array[item]['plain_name']} Thread',
                            daemon=True,
                        )
                        thread.start()
                        threads.append((f'{sequence_array[item]['plain_name']} Thread',thread))
                        status[item]['thread'] = thread
                    else: # stop thread
                        if status[item]['thread'] != None:
                            status[item]['event'].set()
                            status[item]['thread'].join()
                            threads.remove((f'{sequence_array[item]['plain_name']} Thread', status[item]['thread']))
                            status[item]['thread'] = None
                else: # run non thread sequence
                    sequence_array[item]['function'](writer)
            
    except Exception as e:
        _, _, tb = sys.exc_info()
        write_error(f"{master_array[item]['plain_name']} write error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
        print_out(f"{master_array[item]['plain_name']} write error: {type(e).__name__} on line {tb.tb_lineno}: {e}")


# Direct Hardware Functions
def abort(cause='', verbose_cause=True, verbose_abort=True): # A digital abort exists, but cant be activated without an 'abort' switch (except during shutdown)
    if not abort_state.is_set():
        try: 
            abort_state.set() # sets first to stop anything else from writing and interfering with abort
            if daq_system == 'NI': # If NI DAQ
                for writer in ssr_writers: # Iterates through all writers and sets all channels on each module to false
                    writer.write_one_sample_one_line(np.array([False,False,False,False,False,False,False,False]))
                    
            elif daq_system == 'LabJack': # IF LabJack DAQ
                ljm.eWriteNames(handle, len(valve_array), [v["channel"] for v in valve_array.values()], [0]*len(valve_array))
            # Synnax switches being set off are handled when the abort synnax switch is actuated by write_single_switch
            
            if verbose_abort: 
                print_out("\nABORTING\nABORTING\nABORTING\n")
            if cause != '' and verbose_cause:
                print_out(f'Abort: {cause}\n')
    
        except Exception as e:
            _, _, tb = sys.exc_info()
            print_out(f"Abort error: {type(e).__name__} on line {tb.tb_lineno}: {e}")

def unabort(verbose=True): # A digital abort exists, but cant be activated without an 'abort' switch (except during shutdown)
    if abort_state.is_set():
        if verbose:
            print_out("\nUnaborted\n")
        abort_state.clear() # This just unsets the abort_state event

def shutdown(do_abort=True):
    print_out("\nStopping rocket DAQ system...")
    stop_event.set() # Runs this to stop everything else from running and to enable the shutdown process
    try:
        if do_abort: # this is the propper shutdown
            try: # Ensure all relays are turned off on exit
                abort('Shut Down', True, False)
            except Exception as e:
                _, _, tb = sys.exc_info()
                print_out(f'Error Closing Valves: {type(e).__name__} on line {tb.tb_lineno}: {e}')
            time.sleep(.1) # Short wait for NI, caused error when trying to close last ssr task without delay
        else:
            abort_state.set() # This is for special error shutdowns such as the synna cluster stopping while this program is running
        
        safe = True
        unsafe = []
        
        if daq_system == 'NI':
            try:
                for task in ni_tasks: # Closes all the NI tasks
                    devices = [(nidaqmx.system.device.Device(str(task.devices[i])[str(task.devices[i]).find('name=')+5:].rstrip(')')).product_type, (str(task.devices[i])[str(task.devices[i]).find('name=')+5:].rstrip(')'))) for i in range(1, len(task.devices))] # Get the module type and name
                    if len(task.devices) == 2: # Format module type and name 
                        devices_clean = f'{devices[0][0]} {devices[0][1]}'
                    elif len(task.devices) == 3:
                        devices_clean = f'{devices[0][0]} {devices[0][1]} and {devices[1][0]} {devices[1][1]}'
                    elif len(task.devices) > 3:
                        devices_clean = f'{devices[0][0]} {devices[0][1]}'
                        for i in range(1,len(task.devices) - 1):
                            devices_clean = f'{devices_clean}, {devices[i][0]} {devices[i][1]}'
                        devices_clean = f'{devices_clean}, and {devices[-1][0]} {devices[-1][1]}'
                    print_out(f'Closing NI Task for {devices_clean}')
                    task.close()
            except Exception as e:
                _, _, tb = sys.exc_info()
                print_out(f'Error Closing NI Tasks: {type(e).__name__} on line {tb.tb_lineno}: {e}')
        elif daq_system == 'LabJack':
            try: # closes the labjack handle
                ljm.close(handle)
            except Exception as e:
                _, _, tb = sys.exc_info()
                print_out(f'Error Closing LabJack Device: {type(e).__name__} on line {tb.tb_lineno}: {e}')
            
        for name, thread in threads: # stops all running threads (runs after closing ni tasks because threads call ni tasks while running, would cause an error if reversed order)
            thread.join(timeout=5)
            if thread.is_alive():
                print_out(f"WARNING: {name} thread did not stop cleanly")
                safe = False
                unsafe.append(name)
                
        if not save_csv: # Removes csv logs if that setting is set
            print_out('Removing Logs')
            for file in csv_files:
                try:
                    os.remove(file)
                except FileNotFoundError:
                    pass
                except Exception as e:
                    print_out(f'Failed to remove file {file}: {type(e).__name__} on line {tb.tb_lineno}: {e}')
    except Exception as e:
        _, _, tb = sys.exc_info()
        print_out(f"Shutdown Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
            
    if safe: print_out("All systems stopped safely\n\n")
    else: print_out(f"Threads did not stop safely: {unsafe}\n\n")
    terminal_writer.close()
    

# Thread Functions
def data_controller(data_controller_init, master_time_channel):
    try:
        if daq_system == 'NI': # If NI DAQ
            
            pt_modules, tc_modules, lc_modules, ni_reader_tasks, ssr_mods = init_ni()
            
            global pt_pull_freq
            global pt_push_freq
            global tc_pull_freq
            global tc_push_freq
            global lc_pull_freq
            global lc_push_freq
            
            num_pt_chan = sum([mod[1] for mod in pt_modules])
            num_tc_chan = sum([mod[1] for mod in tc_modules])
            num_lc_chan = sum([mod[1] for mod in lc_modules])
            
            # All NI systems will have a continuity check as far as I know, may need to be rewritten for FGS
            continuity_module = sensor_array['Igniter_Cont']['module']
            continuity_channel = sensor_array['Igniter_Cont']['channel']
            continuity_sensor_type = sensor_array['Igniter_Cont']['sensor_type']
            if continuity_sensor_type == 'PT': continuity_modules = pt_modules
            elif continuity_sensor_type == 'TC': continuity_modules = tc_modules
            elif continuity_sensor_type == 'LC': continuity_modules = lc_modules
            
            for mod in continuity_modules: # Add the number of channels before the continuity sensor in the data to the channel to get its true position
                if mod[0] == continuity_module:
                    break
                else:
                    continuity_channel += mod[1]
            # must be int
            pt_update_interval = round(pt_pull_freq / pt_push_freq)
            tc_update_interval = round(tc_pull_freq / tc_push_freq)
            lc_update_interval = round(lc_pull_freq / lc_push_freq)
            
            pt_task, tc_task, lc_task = ni_reader_tasks
            
            # Sets sampling rate as pull_freq and other configs for ni daq, may change if these settings cause error
            pt_task.timing.cfg_samp_clk_timing(rate=pt_pull_freq, sample_mode=AcquisitionType.CONTINUOUS)
            pt_task.in_stream.input_buf_size = pt_pull_freq * 5 
            pt_task.in_stream.overwrite = nidaqmx.constants.OverwriteMode.OVERWRITE_UNREAD_SAMPLES
            pt_reader = stream_readers.AnalogMultiChannelReader(pt_task.in_stream)
            
            tc_task.timing.cfg_samp_clk_timing(rate=tc_pull_freq, sample_mode=AcquisitionType.CONTINUOUS)
            tc_task.in_stream.input_buf_size = tc_pull_freq * 20 
            tc_task.in_stream.overwrite = nidaqmx.constants.OverwriteMode.OVERWRITE_UNREAD_SAMPLES
            tc_reader = stream_readers.AnalogMultiChannelReader(tc_task.in_stream)
            
            lc_task.timing.cfg_samp_clk_timing(rate=lc_pull_freq, sample_mode=AcquisitionType.CONTINUOUS)
            lc_task.in_stream.input_buf_size = lc_pull_freq * 5 
            lc_task.in_stream.overwrite = nidaqmx.constants.OverwriteMode.OVERWRITE_UNREAD_SAMPLES
            lc_reader = stream_readers.AnalogMultiChannelReader(lc_task.in_stream)
            
            # Create lists of all the NI channels to log in propper order
            global pt_channel_order
            global tc_channel_order
            global lc_channel_order
        
            pt_channel_order = [ch.name for ch in pt_task.ai_channels]
            tc_channel_order = [ch.name for ch in tc_task.ai_channels]
            lc_channel_order = [ch.name for ch in lc_task.ai_channels]
            
            # Get scaling factors, name, and synnax channels for all sensors to log
            pt_scaling, pt_names, pt_time_channel, pt_raw_channels, pt_scaled_channels = build_sensor_data('PT')
            tc_scaling, tc_names, tc_time_channel, tc_raw_channels, tc_scaled_channels = build_sensor_data('TC')
            lc_scaling, lc_names, lc_time_channel, lc_raw_channels, lc_scaled_channels = build_sensor_data('LC')
            
            channels = [master_time_channel] + [pt_time_channel] + [tc_time_channel] + [lc_time_channel] + pt_raw_channels + pt_scaled_channels + tc_raw_channels + tc_scaled_channels + lc_raw_channels + lc_scaled_channels
            
            write_channels = [channel for channel in channels if channel is not None]
            
            base_timestamp = get_safe_start_time(master_time_channel, 'Data Controller')
            with client.open_writer(start=base_timestamp, channels=[ch.name for ch in write_channels], enable_auto_commit=True) as writer:
            
                def generic_callback(num_samples, sensor_type, units, reader, num_channels, scaling, names):
                    try: # this is run every time data is pulled from NI
                        buffer = np.zeros((num_channels, num_samples), dtype=np.float64)
                        reader.read_many_sample(buffer, num_samples, timeout=WAIT_INFINITELY)
                        data = buffer.T # convert from arrays for each sensor full of data points for each timestamp to arrays for each timestamp full of data points for each sensor
                        if len(data) == 0:
                            return 0
                        batch_end_time = sy.TimeStamp.now() # timestamp of last data point
                        #print_out(data)

                        #  LOGGING
                        if bool(status['logging']['value']) and do_logging:
                            log_data = []
                            for i in range(len(data)): # Number of data samples
                                sample = data[i]
                                scaled_row = []
                                raw_row = []
                                for j in range(len(names)): # Number of channels
                                    multiplier = 1
                                    offset = 0
                                    if scaling[j] != None: # apply scaling if applicable
                                        multiplier, offset = scaling[j]
                                        raw_row.append(sample[j])
                                    scaled_row.append(sample[j] * multiplier + offset)
                                log_data.append(scaled_row + raw_row)
                            try: # send data to logging 
                                csv_queue.put_nowait({
                                    'time_ns': batch_end_time,
                                    'data': log_data,
                                    'kind': sensor_type,
                                })
                            except Full:
                                pass
                            
                        # Downsample for display
                        if data.shape[0] != 0: # if data is not empty
                            display_data = data[-1, :] # take last data sample
                            try:
                                frame = {f"{sensor_type}_time_channel": np.array([int(batch_end_time)], dtype=np.int64)}
                                for i in range(num_channels):
                                    if names[i] != None: 
                                        multiplier = 1 # if no scaling, use default 
                                        offset = 0 # if no scaling, use default 
                                        if scaling[i] != None: # apply scaling if applicable
                                            multiplier, offset = scaling[i]
                                            frame[f"{sensor_type}_raw_{names[i]}"] = np.array([display_data[i]], dtype=np.float32) # apply raw if there is scaling
                                        scaled_value = float(display_data[i]) * multiplier + offset
                                        frame[f"{sensor_type}_{units}_{names[i]}"] = np.array([scaled_value], dtype=np.float32)
                                        
                                if not stop_event.is_set(): # protection from displaying error on stop
                                    #print_out(frame)
                                    writer.write(frame) # write data to synnax
                                    
                            except Exception as e:
                                _, _, tb = sys.exc_info() 
                                if type(e).__name__ == 'StreamClosed'  or type(e).__name__ == 'ConnectionClosedError'  or type(e).__name__ == 'ConnectionClosedError': # if synnax cluster stops unexpectedly
                                    if not non_abort_shutdown.is_set():
                                        non_abort_shutdown.set()
                                        write_error('CRITICAL ERROR: Synnax Cluster Closed!')
                                        print_out('CRITICAL ERROR: Synnax Cluster Closed!')
                                    time.sleep(.5)
                                else:
                                    write_error(f"{sensor_type} Synnax Write Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
                                    print_out(f"{sensor_type} Synnax Write Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
                                    time.sleep(0.1)

                        # Continuity check: 
                        if sensor_type == continuity_sensor_type:
                            latest_continuity = data[-1, continuity_channel]
                            if continuity_queue.qsize() < continuity_queue.maxsize - 10:
                                continuity_queue.put_nowait(latest_continuity)
                    except DaqReadError as e:
                        _, _, tb = sys.exc_info()
                        write_error(f"{sensor_type} generic callback DaqReadError: on line {tb.tb_lineno}: {e}")
                        print_out(f"{sensor_type} generic callback DaqReadError: on line {tb.tb_lineno}: {e}")
                    except Exception as e:
                        _, _, tb = sys.exc_info()
                        if type(e).__name__ == 'StreamClosed'  or type(e).__name__ == 'ConnectionClosedError': # if synnax cluster stops unexpectedly # if synnax cluster stops unexpectedly
                            if not non_abort_shutdown.is_set():
                                non_abort_shutdown.set()
                                write_error('CRITICAL ERROR: Synnax Cluster Closed!')
                                print_out('CRITICAL ERROR: Synnax Cluster Closed!')
                            time.sleep(.5)
                        else:
                            write_error(f"{sensor_type} generic callback error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
                            print_out(f"{sensor_type} generic callback error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
                
                # NI handles daq by calling a callback function for each task. because we cant pass data to the callback, we call different callbacks, which then pass data to a generic callback
                def pt_callback(task_idx, event_type, num_samples, cb_data=None): 
                    try:
                        if not stop_event.is_set(): # protection from displaying error on stop
                            generic_callback(num_samples, 'PT', pt_units, pt_reader, num_pt_chan, pt_scaling, pt_names)
                    except Exception as e:
                        if not stop_event.is_set(): # protection from displaying error on stop
                            _, _, tb = sys.exc_info()
                            if type(e).__name__ == 'StreamClosed'  or type(e).__name__ == 'ConnectionClosedError': # if synnax cluster stops unexpectedly
                                if not non_abort_shutdown.is_set():
                                    non_abort_shutdown.set()
                                    print_out('CRITICAL ERROR: Synnax Cluster Closed!')
                                time.sleep(.5)
                            else:
                                write_error(f"PT callback error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
                                print_out(f"PT callback error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
                            return -1
                    return 0
                
                def tc_callback(task_idx, event_type, num_samples, cb_data=None): 
                    try:
                        if not stop_event.is_set(): # protection from displaying error on stop
                            generic_callback(num_samples, 'TC', tc_units, tc_reader, num_tc_chan, tc_scaling, tc_names)
                    except Exception as e:
                        if not stop_event.is_set(): # protection from displaying error on stop
                            _, _, tb = sys.exc_info()
                            if type(e).__name__ == 'StreamClosed'  or type(e).__name__ == 'ConnectionClosedError': # if synnax cluster stops unexpectedly
                                if not non_abort_shutdown.is_set():
                                    non_abort_shutdown.set()
                                    print_out('CRITICAL ERROR: Synnax Cluster Closed!')
                                time.sleep(.5)
                            else:
                                write_error(f"TC callback error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
                                print_out(f"TC callback error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
                            return -1
                    return 0
                
                def lc_callback(task_idx, event_type, num_samples, cb_data=None): 
                    try:
                        if not stop_event.is_set(): # protection from displaying error on stop
                            generic_callback(num_samples, 'LC', lc_units, lc_reader, num_lc_chan, lc_scaling, lc_names)
                    except Exception as e:
                        if not stop_event.is_set(): # protection from displaying error on stop
                            _, _, tb = sys.exc_info()
                            if type(e).__name__ == 'StreamClosed'  or type(e).__name__ == 'ConnectionClosedError': # if synnax cluster stops unexpectedly
                                if not non_abort_shutdown.is_set():
                                    non_abort_shutdown.set()
                                    print_out('CRITICAL ERROR: Synnax Cluster Closed!')
                                time.sleep(.5)
                            else:
                                write_error(f"LC callback error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
                                print_out(f"LC callback error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
                            return -1
                    return 0
                
                # Wait for logging to be propperly populated before continuing
                log_init = False
                time_later = False
                while not log_init or not time_later:
                    if 'logging' in status: 
                        if 'value' in status['logging']:
                            log_init = True
                    if sy.TimeStamp.now() >= base_timestamp:
                        time_later = True
                    time.sleep(.1)
                
                # Groups update_interval number of samples for callback
                pt_task.register_every_n_samples_acquired_into_buffer_event(pt_update_interval, pt_callback)
                tc_task.register_every_n_samples_acquired_into_buffer_event(tc_update_interval, tc_callback)
                lc_task.register_every_n_samples_acquired_into_buffer_event(lc_update_interval, lc_callback)
                
                pt_task.start()
                tc_task.start()
                lc_task.start()
                
                data_controller_init.set()
                print_out(f"Data Controller Thread Running")
                
                try:
                    while not stop_event.is_set(): # we dont need to do anything in the loop because the NI tasks will be running the callback functions
                        time.sleep(.1)
                except Exception as e:
                    _, _, tb = sys.exc_info()
                    write_error(f"Data Controller Read Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
                    print_out(f"Data Controller Read Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
                finally:
                    writer.write({master_time_channel.name: sy.TimeStamp.now()})
        
        elif daq_system == 'LabJack': # If LabJack DAQ
            # LabJack Config
            ljm.eWriteName(handle, "STREAM_TRIGGER_INDEX", 0) # Ensure triggered stream is disabled.
            ljm.eWriteName(handle, "STREAM_CLOCK_SOURCE", 0) # Enabling internally-clocked stream.
            
            # AIN ranges are +/-10 V and stream resolution index is 0 (default).
            aNames = ["AIN_ALL_RANGE", "STREAM_RESOLUTION_INDEX"]
            aValues = [10.0, 0]

            # set to single ended and auto settling time
            aNames.extend(["AIN_ALL_NEGATIVE_CH", "STREAM_SETTLING_US"])
            aValues.extend([ljm.constants.GND, 0])
            
            aScanListNames = [sensor_array[sensor]['channel'] for sensor in sensor_array]
            numAddresses = len(aScanListNames)
            aScanList = ljm.namesToAddresses(numAddresses, aScanListNames)[0]
            
            sensor_update_interval = round(sensor_pull_freq/sensor_push_freq)
            
            
            num_pt_chan = sum(1 for v in sensor_array.values() if v.get('sensor_type') == 'PT')
            num_tc_chan = sum(1 for v in sensor_array.values() if v.get('sensor_type') == 'TC')
            num_lc_chan = sum(1 for v in sensor_array.values() if v.get('sensor_type') == 'LC')
        
            # get the sensors for each sensor type
            pt_channel_order = [sensor_array[sensor]['channel'] for sensor in sensor_array if sensor_array[sensor]['sensor_type'] == 'PT']
            tc_channel_order = [sensor_array[sensor]['channel'] for sensor in sensor_array if sensor_array[sensor]['sensor_type'] == 'TC']
            lc_channel_order = [sensor_array[sensor]['channel'] for sensor in sensor_array if sensor_array[sensor]['sensor_type'] == 'LC']
            
            # Get scaling factor, name, and synnax channels for all sensors to log
            pt_scaling, pt_names, pt_time_channel, pt_raw_channels, pt_scaled_channels = build_sensor_data('PT')
            tc_scaling, tc_names, tc_time_channel, tc_raw_channels, tc_scaled_channels = build_sensor_data('TC')
            lc_scaling, lc_names, lc_time_channel, lc_raw_channels, lc_scaled_channels = build_sensor_data('LC')
            
            channels = [master_time_channel] + [pt_time_channel] + [tc_time_channel] + [lc_time_channel] + pt_raw_channels + pt_scaled_channels + tc_raw_channels + tc_scaled_channels + lc_raw_channels + lc_scaled_channels
            
            write_channels = [channel for channel in channels if channel is not None]
            
            base_timestamp = get_safe_start_time(master_time_channel, 'Data Controller')
            with client.open_writer(start=base_timestamp, channels=[ch.name for ch in write_channels], enable_auto_commit=True) as writer:
                
                def process_data(buffer , batch_end_time, sensor_type, units, num_channels, scaling, names):
                    try: # similar to NI generic callback, runs for each sensor type for each data pull
                        data = buffer.T # convert from arrays for each sensor full of data points for each timestamp to arrays for each timestamp full of data points for each sensor
                        
                        #  LOGGING
                        if bool(status['logging']['value']) and do_logging:
                            log_data = []
                            for i in range(len(data)): # Number of data samples
                                sample = data[i]
                                scaled_row = []
                                raw_row = []
                                for j in range(len(names)): # Number of channels
                                    multiplier = 1
                                    offset = 0
                                    if scaling[j] != None: # apply scaling if applicable
                                        multiplier, offset = scaling[j]
                                        raw_row.append(sample[j])
                                    scaled_row.append(sample[j] * multiplier + offset)
                                log_data.append(scaled_row + raw_row)
                            try: # send data to logging 
                                csv_queue.put_nowait({
                                    'time_ns': batch_end_time,
                                    'data': log_data,
                                    'kind': sensor_type,
                                })
                            except Full:
                                pass
                            
                        # Downsample for display
                        if data.shape[0] != 0: # if data is not empty
                            display_data = data[-1, :] # take last data sample
                            try:
                                frame = {f"{sensor_type}_time_channel": np.array([int(batch_end_time)], dtype=np.int64)}
                                for i in range(num_channels):
                                    if names[i] != None: 
                                        multiplier = 1 # if no scaling, use default 
                                        offset = 0 # if no scaling, use default 
                                        if scaling[i] != None: # apply scaling if applicable
                                            multiplier, offset = scaling[i]
                                            frame[f"{sensor_type}_raw_{names[i]}"] = np.array([display_data[i]], dtype=np.float32) # apply raw if there is scaling
                                        scaled_value = float(display_data[i]) * multiplier + offset
                                        frame[f"{sensor_type}_{units}_{names[i]}"] = np.array([scaled_value], dtype=np.float32)
                                        
                                if not stop_event.is_set(): # protection from displaying error on stop
                                    #print_out(frame)
                                    writer.write(frame) # write data to synnax
                                        
                                if not stop_event.is_set(): # protection from displaying error on stop
                                    #print_out(frame)
                                    writer.write(frame)
                                    
                            except Exception as e:
                                _, _, tb = sys.exc_info() 
                                if type(e).__name__ == 'StreamClosed'  or type(e).__name__ == 'ConnectionClosedError': # if synnax cluster stops unexpectedly
                                    if not non_abort_shutdown.is_set():
                                        non_abort_shutdown.set()
                                        print_out('CRITICAL ERROR: Synnax Cluster Closed!')
                                    time.sleep(.5)
                                else:
                                    print_out(f"{sensor_type} Synnax Write Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
                                    time.sleep(0.1)
                
                    except Exception as e:
                        _, _, tb = sys.exc_info()
                        if type(e).__name__ == 'StreamClosed'  or type(e).__name__ == 'ConnectionClosedError': # if synnax cluster stops unexpectedly
                            if not non_abort_shutdown.is_set():
                                non_abort_shutdown.set()
                                print_out('CRITICAL ERROR: Synnax Cluster Closed!')
                            time.sleep(.5)
                        else:
                            print_out(f"{sensor_type} data processing error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
                
                # Wait for logging to be propperly populated before continuing
                log_init = False
                time_later = False
                while not log_init or not time_later:
                    if 'logging' in status: 
                        if 'value' in status['logging']:
                            log_init = True
                    if sy.TimeStamp.now() >= base_timestamp:
                        time_later = True
                    time.sleep(.1)
            
                ljm.eStreamStart(handle, sensor_update_interval, numAddresses, aScanList, sensor_pull_freq)
                
                data_controller_init.set()
                print_out(f"Data Controller Thread Running")
                    
                try:
                    while not stop_event.is_set(): 
                        ret = ljm.eStreamRead(handle) # get data from labjack
                        data = ret[0]
                        batch_end_time = sy.TimeStamp.now()
                        
                        deinterleaved = [data[index::numAddresses] for index in range(numAddresses)] # labjack returns a 1D arary, convert to 2d
                        data_array = np.array(deinterleaved)
                        
                        for sensor_type in ['PT', 'TC', 'LC']:
                            if sensor_type == 'PT': 
                                channel_order = pt_channel_order
                                units = pt_units
                                num_channels = num_pt_chan
                                scaling = pt_scaling
                                names = pt_names
                            elif sensor_type == 'TC': 
                                channel_order = tc_channel_order
                                units = tc_units
                                num_channels = num_tc_chan
                                scaling = tc_scaling
                                names = tc_names
                            elif sensor_type == 'LC': 
                                channel_order = lc_channel_order
                                units = lc_units
                                num_channels = num_lc_chan
                                scaling = lc_scaling
                                names = lc_names
                            
                            sensor_data = []
                            for channel in channel_order:
                                chanNumAbs = aScanListNames.index(channel)
                                chanNumSensor = channel_order.index(channel)
                                channelData = data_array[chanNumAbs]
                                sensor_data.append(channelData)
                            sensor_data = np.array(sensor_data)
                            
                            process_data(sensor_data, batch_end_time, sensor_type, units, num_channels, scaling, names)
                        
                except Exception as e:
                    _, _, tb = sys.exc_info()
                    print_out(f"Data Controller Read Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
                finally:
                    ljm.eStreamStop(handle)
                    writer.write({master_time_channel.name: sy.TimeStamp.now()})   
        
    except Exception as e:
        _, _, tb = sys.exc_info()
        print_out(f"Data Controller Setup Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
    finally:
        print_out(f"Data Controller Thread Stopped")

def valve_controller(valve_controller_init, master_time_channel):
    try:        
        loop = sy.Loop(sy.Rate.HZ * 10) # 10 Hz
        
        readChannels = []
        writeChannels = []
        
        for switch in master_array: # populate channels for writer and streamer and populates status
            state, cmd, state_time, cmd_time = create_synnax_switch_channels(switch)
            value = int(master_array[switch]['state'])
            readChannels.append(cmd)
            readChannels.append(cmd_time)
            writeChannels.append(state)
            writeChannels.append(state_time)
            status[switch] = {'state':state,'cmd':cmd,'state_time':state_time,'cmd_time':cmd_time,'value':value}
            if switch in sequence_array:
                if sequence_array[switch]['thread'] == True:
                    status[switch]['thread'] = None 
                    status[switch]['event'] = threading.Event()
        
        # Countdown and continuity channels for hotfire (just initializing them here for the writer, will be recreated by hotfire sequence)
        countdown_time_channel = client.channels.create(
            name="countdown_time",
            is_index=True,
            data_type=sy.DataType.TIMESTAMP,
            retrieve_if_name_exists=True
        )
        countdown_channel = client.channels.create(
            name="countdown",
            data_type=sy.DataType.FLOAT32,
            index=countdown_time_channel.key,
            retrieve_if_name_exists=True
        )
       
        writeChannels.append(countdown_time_channel.name)
        writeChannels.append(countdown_channel.name)
        
        base_timestamp = get_safe_start_time(master_time_channel, 'Valve Controller')
        
        with client.open_streamer(readChannels) as streamer, client.open_writer(start=base_timestamp, channels=writeChannels, enable_auto_commit=True) as writer:
            try:
                # Write Initial Conditions
                for switch in status:
                    write_single_switch(writer, switch, status[switch]['value'], verbose=False)
                
                writer.write({countdown_time_channel.name:sy.TimeStamp.now(),countdown_channel.name:abs(countdown_clock)})
                
                valve_controller_init.set()
                
                print_out(f"Valve Controller Thread Running")
                
                while not stop_event.is_set() and loop.wait():
                    try:
                        frame = streamer.read(timeout=0.1) # Gets states from synnax
                        shift_pressed = True if bypass_shift else keyboard.is_pressed('shift')
                        if frame is not None:
                            latest_time = sy.TimeStamp.now()
                            if shift_pressed:
                                for switch in master_array: # checks if any switches have changed, uses priority order from master_array
                                    data = frame.get(status[switch]['cmd'])
                                    if data is not None and len(data) > 0:
                                        latest_data = data[-1] # latest synnax state
                                        if latest_data != status[switch]['value']: # if changed
                                            if switch == 'abort': # Abort switch is not blocked by the abort and runs special code
                                                if not bool(latest_data): 
                                                    abort('Digital Abort')
                                                    write_single_switch(writer, switch, latest_data)
                                                else:
                                                    unabort(abort_state)
                                                    write_single_switch(writer, switch, latest_data)
                                            elif switch in abort_independent_switches: # any switches independent from abort state (ex logging)
                                                write_single_switch(writer, switch, latest_data)
                                            elif not abort_state.is_set(): # all other switches
                                                write_single_switch(writer, switch, latest_data)
                                            elif print_switch_changes: # all other switches if abort is triggered and printing
                                                if switch in valve_array: name = valve_array[switch]['plain_name']
                                                elif switch in sequence_array: name = sequence_array[switch]['plain_name']
                                                elif switch in lockout_array: name = lockout_array[switch]['plain_name']
                                                if switch in valve_array:
                                                    print_out(f'{name} set to {valve_array[switch]['nominal']}. Abort in effect')
                                                else:
                                                    print_out(f'{name} set to Off. Abort in effect')
                                                # reset synnax switch
                                                writer.write({status[switch]['state']:latest_data,status[switch]['state_time']: latest_time})
                                                writer.write({status[switch]['state']:int(not bool(latest_data)),status[switch]['state_time']: sy.TimeStamp.now()})
                                                
                            else: # switch not pressed
                                print_out('Shift not pressed. Ignoring')
                                for switch in master_array: 
                                    data = frame.get(status[switch]['cmd'])
                                    if data is not None and len(data) > 0:
                                        latest_data = data[-1]
                                        if latest_data != status[switch]['value']: # if switch changed, reset the synnax switch
                                            writer.write({status[switch]['state']:latest_data,status[switch]['state_time']: latest_time})
                                            writer.write({status[switch]['state']:int(not bool(latest_data)),status[switch]['state_time']: sy.TimeStamp.now()})
                                
                    except Exception as e:
                        if not stop_event.is_set():
                            _, _, tb = sys.exc_info()
                            if type(e).__name__ == 'StreamClosed'  or type(e).__name__ == 'ConnectionClosedError': # if synnax cluster stops unexpectedly
                                if not non_abort_shutdown.is_set():
                                    non_abort_shutdown.set()
                                write_error('CRITICAL ERROR: Synnax Cluster Closed!')
                                print_out('CRITICAL ERROR: Synnax Cluster Closed!')
                                time.sleep(.5)
                            else:
                                write_error(f"Valve Controller Control Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
                                print_out(f"Valve Controller Control Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
                                break
                    
            except Exception as e:
                if not stop_event.is_set():
                    _, _, tb = sys.exc_info()
                    print_out(f"Valve Controller Initial Write Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
            finally: # write nominal states for all synnax switches
                current_time = sy.TimeStamp.now()
                synnax_state = {countdown_time_channel.name:current_time,countdown_channel.name:abs(-10)}
                for switch in status: 
                    synnax_state[status[switch]['state']] = 0
                    synnax_state[status[switch]['state_time']] = current_time
                writer.write(synnax_state)
            
    except Exception as e:
        _, _, tb = sys.exc_info()
        if type(e).__name__ == 'StreamClosed'  or type(e).__name__ == 'ConnectionClosedError': # if synnax cluster stops unexpectedly
            if not non_abort_shutdown.is_set():
                non_abort_shutdown.set()
        else:
            print_out(f"Valve Controller Setup error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
    finally: 
        print_out(f"Valve Controller Thread Stopped")


def main():
    check_user_values()
    
    master_time_channel = client.channels.create(
        name="master_time",
        is_index=True,
        data_type=sy.DataType.TIMESTAMP,
        retrieve_if_name_exists=True,
    )
    
    data_controller_init = threading.Event()
    valve_controller_init = threading.Event()
    
    # Create threads
    thread_configs = [
        ("Data Controller", data_controller, (data_controller_init, master_time_channel)),
        ("Valve Controller", valve_controller, (valve_controller_init, master_time_channel)),
    ]
    
    for name, target, args in thread_configs:
        try:
            thread = threading.Thread(target=target, args=args, name=name)
            thread.daemon = True
            threads.append((name, thread))
            thread.start()
            print_out(f"Started {name} thread")
        except Exception as e:
            _, _, tb = sys.exc_info()
            print_out(f"Failed to start {name} thread: {type(e).__name__} on line {tb.tb_lineno}: {e}")
            
    while not (data_controller_init.is_set() and valve_controller_init.is_set()): # wait for all threads to initialize 
        time.sleep(.1)
        
    try:
        num_pt_chan = sum(1 for v in sensor_array.values() if v.get('sensor_type') == 'PT')
        num_tc_chan = sum(1 for v in sensor_array.values() if v.get('sensor_type') == 'TC')
        num_lc_chan = sum(1 for v in sensor_array.values() if v.get('sensor_type') == 'LC')
        if daq_system == 'NI':
            global pt_pull_freq
            global pt_push_freq
            global tc_pull_freq
            global tc_push_freq
            global lc_pull_freq
            global lc_push_freq
        elif daq_system == 'LabJack':
            pt_pull_freq = sensor_pull_freq
            pt_push_freq = sensor_push_freq
            tc_pull_freq = sensor_pull_freq
            tc_push_freq = sensor_push_freq
            lc_pull_freq = sensor_pull_freq
            lc_push_freq = sensor_push_freq
        
        base_timestamp = get_safe_start_time(master_time_channel, 'Main')        
        with client.open_writer(start=base_timestamp, channels=master_time_channel.name, enable_auto_commit=True) as writer: 
            # write initial display with settings
            print_out(f"\n\nROCKET DAQ SYSTEM STARTED\nStarted At: {time.strftime("%Y-%m-%d %H:%M:%S")}\n")
            print_out("=" * 2 * shutil.get_terminal_size().columns)
            print_out(f"PT: {num_pt_chan} channels, {pt_pull_freq}Hz data → {pt_push_freq}Hz display")
            print_out(f"TC: {num_tc_chan} channels, {tc_pull_freq}Hz data → {tc_push_freq}Hz display")
            print_out(f"LC: {num_lc_chan} channels, {lc_pull_freq}Hz data → {lc_push_freq}Hz display")
            if do_logging: 
                print_out("Raw NI data logged to CSV files")
            print_out("=" * 2 * shutil.get_terminal_size().columns)
            print_out("\nPress Ctrl+C to stop...\n\n")
            
            interval = 5 # Check every x seconds
            while not stop_event.is_set():
                try:
                    start = time.perf_counter()
                    
                    if 'hotfire' in status:
                        if status['hotfire']['event'].is_set(): # Remove aborted or completed hotfire threads and turn off the switch (special case)
                            if status['hotfire']['thread'] != None:
                                time.sleep(1)
                                status['hotfire']['thread'].join()
                                threads.remove((f'{sequence_array['hotfire']['plain_name']} Thread', status['hotfire']['thread']))
                                status['hotfire']['thread'] = None
                            
                    if abort_state.is_set(): # Remove sequence threads after an abort
                        for seq in sequence_array:
                            if sequence_array[seq]['thread'] == True:
                                if status[seq]['thread'] != None:
                                    status[seq]['event'].set()
                                    status[seq]['thread'].join()
                                    threads.remove((f'{sequence_array[seq]['plain_name']} Thread', status[seq]['thread']))
                                    status[seq]['thread'] = None
                                    
                    # Check for dead threads
                    dead_threads = [(name, t) for name, t in threads if not t.is_alive()]
                    if dead_threads:
                        print_out(f"WARNING: Dead threads detected: {[name for name, _ in dead_threads]}")
                    
                    try:
                        writer.write({master_time_channel.name:sy.TimeStamp.now()})
                    except Exception as e:
                        if type(e).__name__ == 'StreamClosed'  or type(e).__name__ == 'ConnectionClosedError': # if synnax cluster stops unexpectedly
                            pass
                        else:
                            raise
                    
                    while (time.perf_counter() - start < interval) and not stop_event.is_set(): # wait for next interval
                        if non_abort_shutdown.is_set(): # check for special conditions
                            if not stop_event.is_set():
                                shutdown(do_abort=False)
                        time.sleep(.05)
                        
                except Exception as e:
                    _, _, tb = sys.exc_info()
                    if type(e).__name__ == 'StreamClosed'  or type(e).__name__ == 'ConnectionClosedError': # if synnax cluster stops unexpectedly
                        if non_abort_shutdown.is_set():
                            if not stop_event.is_set():
                                print_out('CRITICAL ERROR: Synnax Cluster Closed!')
                                shutdown(do_abort=False)
                            time.sleep(.5)
                    else:
                        print_out(f"Health Monitor Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")# write initial display with settings
                        time.sleep(1)
            try:
                writer.write({master_time_channel.name:sy.TimeStamp.now()}) # write final master_time
            except:
                pass 
            if not stop_event.is_set():
                shutdown()
            
    except KeyboardInterrupt as e:
        if not stop_event.is_set():
            shutdown()

    except Exception as e:
        _, _, tb = sys.exc_info()
        print_out(f"Main Thread Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
        if not stop_event.is_set():
            shutdown()



if __name__ == '__main__':
    main()

'''
COMPLETED:
    
    
TO DO
High Priority: 
    Verify valve actuation works on NI (highly likely, 99% chance it works) (use dsub breakout board or bread board for test!)
    Test changes to ssr channel writing and labjack channel writing
    Test dual_mpva and hotfire with dual_mpva (ask how precise lead time needs to be and if we need lead time at the end of the burn. simulated NI is about 1/4 ms longer than lead time, get labjack too). 
    Add extra run conditions to hotfire (dual_mpvas, mfv, mov, n2purge, igniter)
    Flow Stand Configuration
    Torch Igniter Configuration
    Valve State Logging
Medium Priority:
    Go thru procedure to add more safety checks (igniter cond_false hotfire and hotfire lock, hotfire manually sets igniter? check for spaces or invalid characters in names)
    Test real NI on linux
Low Priority:

'''