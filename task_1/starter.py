'''
стартер — запускает все процессы системы
'''
import os
import sys
import subprocess

data_path = str(sys.argv[1])
workers_number = int(sys.argv[2])


etl = 'ETL.py'
manager = 'manager.py'
client = 'client.py'
worker = 'worker.py'

subprocess.Popen(['python3', etl, data_path, str(workers_number)])

for i in range(workers_number):
    subprocess.Popen(['python3', worker, str(i)])

subprocess.Popen(['python3', manager])
