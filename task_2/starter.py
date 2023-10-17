'''
стартер — запускает все процессы системы
'''
import os
import sys
import subprocess

# process args
if len(sys.argv) < 4:
    print("[S] Expected at least 3 arguments: startup_setting_path workers_number simulator_delay")
    exit(1)

workers_number_str = sys.argv[2]

if not workers_number_str.isnumeric():
    print("[S] Workers_number must be integer")
    exit(1)

workers_number = int(workers_number_str)

if workers_number <= 0:
    print("[S] Workers_number must be positive")
    exit(1)


simulator_delay_str = sys.argv[2]

if not simulator_delay_str.isnumeric():
    print("[S] Simulator_delay must be integer")
    exit(1)

simulator_delay = int(simulator_delay_str)

if simulator_delay <= 0:
    print("[S] Simulator_delay must be positive")
    exit(1)

startup_settings_path = str(sys.argv[1])

etl = 'ETL.py'
manager = 'manager.py'
worker = 'worker.py'
communication_simulator = 'communication_simulator.py'

subprocess.Popen(['python3', etl, workers_number_str, startup_settings_path])

for i in range(workers_number):
    subprocess.Popen(['python3', worker, str(i), startup_settings_path])

subprocess.Popen(['python3', manager, startup_settings_path])
subprocess.Popen(['python3', communication_simulator, simulator_delay_str])
