from midgard_functions import get_element, combine_with_and, label, check_configs, write_actuation, abort, unabort, shutdown
import time

def run(global_vars, element):
    debug_mode, show_server_logs, simulated_data, log_data, log_actuations, print_switch_changes, keep_csv, openmct_dir, log_output_dir, confirmation_keys, peripherals, threads, servers, stop_event, abort_state, non_abort_shutdown, startup_event, sync_groups, data_tree, all_keys, openmct, telemetry = global_vars
    
    print('SEQUENCE RUNNING')
    last_time = time.time()
    while not element.stop_event.is_set() and not stop_event.is_set():
        current_time = time.time()
        if current_time-last_time >=5:
            last_time = current_time
            print('SEQUENCE RUNNING')
        time.sleep(0.01)
    print('SEQUENCE STOPPED')
