from midgard_functions import gv, print_out, error_out, get_element, combine_with_and, label, run, check_and_install_openmct, check_configs, write_actuation, abort, unabort, shutdown
import time, sys

def run(element):
    try:
        print_out('SEQUENCE RUNNING')
        last_time = time.time()
        while not element.stop_event.is_set() and not gv.stop_event.is_set():
            current_time = time.time()
            if current_time-last_time >=5:
                last_time = current_time
                print_out('SEQUENCE RUNNING')
            time.sleep(0.01)
        print_out('SEQUENCE STOPPED')
    except Exception as e:
        _, _, tb = sys.exc_info()
        print_out(f"Test Sequence Error: {type(e).__name__} on line {tb.tb_lineno}: {e}")
