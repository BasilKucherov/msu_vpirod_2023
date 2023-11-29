'''
стартер — запускает все процессы системы
'''
import argparse
import os
import sys
import subprocess

# argparser
def check_positive(numeric_type):
    def require_positive(value):
        number = numeric_type(value)
        if number <= 0:
            raise argparse.ArgumentTypeError(f"Number {value} must be positive.")
        return number

    return require_positive

def check_positive_or_zero(numeric_type):
    def require_positive_or_zero(value):
        number = numeric_type(value)
        if number < 0:
            raise argparse.ArgumentTypeError(f"Number {value} must be positive or zero.")
        return number

    return require_positive_or_zero

def check_path(path):
    def path_exists(path):
        if not os.path.isfile(path):
            raise argparse.ArgumentTypeError(f"string \'{path}\' must be existing file path.")
        return path

    return path_exists

parser = argparse.ArgumentParser(description='Start all system processes')
parser.add_argument('-wn', '--workers-number', type=check_positive(int),
                    help='number of workers (who stores data)')
parser.add_argument('-cn', '--coordinators-number', type=check_positive(int),
                    help='number of coordinators')
parser.add_argument('-cd', '--communicator-delay', type=check_positive_or_zero(int),
                    help='coomunicator delay')
parser.add_argument('-p', '--settings-path', type=check_path(str),
                    help='settings path')

args = parser.parse_args()
args.coordinators_number
args.communicator_delay
args.settings_path

etl = 'ETL.py'
manager_coordinator = 'manager_coordinator.py'
worker = 'worker.py'
communication_simulator = 'communication_simulator.py'

subprocess.Popen(['python3', etl, str(args.workers_number),
                   str(args.coordinators_number), args.settings_path])

for i in range(args.workers_number):
    subprocess.Popen(['python3', worker, str(i)])

for i in range(args.coordinators_number):
    subprocess.Popen(['python3', manager_coordinator, str(i)])

subprocess.Popen(['python3', communication_simulator, 
                  str(args.communicator_delay), str(args.workers_number)])

sys.exit(0)
