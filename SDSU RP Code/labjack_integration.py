import time, csv, os, keyboard, threading, sys, shutil, re
import numpy as np
from queue import Queue, Empty, Full
import synnax as sy
from labjack import ljm

numChannels = 6

FIRST_AIN_CHANNEL = 0  # 0 = AIN0
NUMBER_OF_AINS = numChannels


# Synnax Setup
client = sy.Synnax(
    host="localhost",
    port=9090,
    username="synnax",
    password="seldon",
    secure=False
)

handle = ljm.openS("T7", "ANY", "ANY")  # T7 device, Any connection, Any identifier
info = ljm.getHandleInfo(handle)
deviceType = info[0]

try:
    # LabJack Config
    ljm.eWriteName(handle, "STREAM_TRIGGER_INDEX", 0) # Ensure triggered stream is disabled.
    ljm.eWriteName(handle, "STREAM_CLOCK_SOURCE", 0) # Enabling internally-clocked stream.
    
    # AIN ranges are +/-10 V and stream resolution index is 0 (default).
    aNames = ["AIN_ALL_RANGE", "STREAM_RESOLUTION_INDEX"]
    aValues = [10.0, 0]

    # set to single ended and auto settling time
    aNames.extend(["AIN_ALL_NEGATIVE_CH", "STREAM_SETTLING_US"])
    aValues.extend([ljm.constants.GND, 0])

    # Stream configuration
    aScanListNames = ["AIN%i" % i for i in range(FIRST_AIN_CHANNEL, FIRST_AIN_CHANNEL + NUMBER_OF_AINS)]  # Scan list names
    print("\nScan List = " + " ".join(aScanListNames))
    numAddresses = len(aScanListNames)
    aScanList = ljm.namesToAddresses(numAddresses, aScanListNames)[0]
    print(aScanList)
    scanRate = 10
    print("scanRate:", scanRate)
    scansPerRead = int(scanRate)

    # Configure and start stream
    scanRate = ljm.eStreamStart(handle, scansPerRead, numAddresses, aScanList, scanRate)
    
    #ljm.eWriteName(handle, 'FIO0', 0)
    cache1=[]
    cache2 = []
    start_time = time.time()
    #while (time.time() - start_time < 3):
    ret = ljm.eStreamRead(handle)
    data = ret[0]
    deinterleaved = [data[idx::numAddresses] for idx in range(numAddresses)]
    data_array = np.array(deinterleaved)
    for i in range(len(data_array)):
        print(i)
        print(data_array[i])
        print()
    print('new')    
    ret = ljm.eStreamRead(handle)
    data = ret[0]
    deinterleaved = [data[idx::numAddresses] for idx in range(numAddresses)]
    data_array = np.array(deinterleaved)
    for i in range(len(data_array)):
        print(i)
        print(data_array[i])
        print()

    time_col = np.full((len(data_array[0]), 1), int(sy.TimeStamp.now()))

    # transpose data so its organized correctly for csv
    all_data = np.hstack((time_col, data_array.T))
    #cache1.append(data_array)
    #cache2.append(all_data)
    #print(all_data,'\n\n')
    
    '''ljm.eWriteName(handle, 'FIO0', 1)
    time.sleep(1)
    ljm.eWriteName(handle, 'FIO0', 0)
    time.sleep(1)
    ljm.eWriteName(handle, 'FIO0', 1)
    time.sleep(1)
    ljm.eWriteName(handle, 'FIO0', 0)
    time.sleep(1)
    ljm.eWriteName(handle, 'FIO0', 1)
    time.sleep(1)
    ljm.eWriteName(handle, 'FIO0', 0)
    time.sleep(1)
    ljm.eWriteName(handle, 'FIO0', 0)'''
    
    '''file1 = open('data1','w')
    file1.write(str(cache1))
    file1.close()
    file2 = open('data2','w')
    file2.write(str(cache2))
    file2.close()'''
    
except ljm.LJMError:
    ljme = sys.exc_info()[1]
    print(ljme)
except Exception:
    e = sys.exc_info()[1]
    print(e)

try:
    print("\nStop Stream")

    ljm.eStreamStop(handle)

except ljm.LJMError:
    ljme = sys.exc_info()[1]
    print(ljme)
except Exception:
    e = sys.exc_info()[1]
    print(e)


ljm.close(handle)