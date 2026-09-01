import time

def run(stop_event, element):
    print('SEQUENCE RUNNING')
    last_time = time.time()
    while not element.stop_event.is_set() and not stop_event.is_set():
        current_time = time.time()
        if current_time-last_time >=5:
            last_time = current_time
            print('SEQUENCE RUNNING')
        time.sleep(0.01)
    print('SEQUENCE STOPPED')
