'''
Процесс ETL (extract, transform, load) – load vars from file and send to workers and manager
'''
import sys
import json
import pika
import numpy as np

# RabbitMQ
RABBITMQ_HOST = 'localhost'

RABBITMQ_QUEUE_TO_COORDINATOR_PREFIX = 'to_coordinator_'
RABBITMQ_QUEUE_TO_WORKER_PREFIX = 'to_worker_'

# get args
workers_number = int(sys.argv[1])
coordinators_number = int(sys.argv[2])
startup_setting_path = sys.argv[3]

print(f"[ETL] coordinators: {coordinators_number}, workers: {workers_number}")

# connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.exchange_declare(exchange='workers', exchange_type='fanout')
channel.exchange_declare(exchange='coordinators', exchange_type='fanout')

# Queues
for worker_id in range(workers_number):
    queue_name = f'{RABBITMQ_QUEUE_TO_WORKER_PREFIX}{worker_id}'
    channel.queue_declare(queue=queue_name)
    channel.queue_bind(exchange='workers', queue=queue_name)

for coordinator_id in range(coordinators_number):
    queue_name = f'{RABBITMQ_QUEUE_TO_COORDINATOR_PREFIX}{coordinator_id}'
    channel.queue_declare(queue=queue_name)
    channel.queue_bind(exchange='coordinators', queue=queue_name)

# process settings
def process_settings(settings, coordinators_number):
    new_settings = {}

    new_settings["variables"] = settings["variables"]
    new_settings["ping_timeout"] = settings["ping_timeout"]

    leader_id = 0
    if settings["leader_id"] == "random":
        leader_id = int(np.random.randint(coordinators_number))
    else:
        leader_id = int(settings["leader_id"])
    new_settings["leader_id"] = leader_id

    ping_delay = int(np.random.randint(settings["ping_delay_min"], settings["ping_delay_max"]))
    new_settings["ping_delay"] = ping_delay

    ring_sequence = None
    match settings["ring_sequence"]:
        case "random":
            ring_sequence = np.random.permutation(np.arange(0, coordinators_number))
        case "ascending":
            ring_sequence = np.arange(0, coordinators_number)
        case "descending":
            ring_sequence = np.arange(coordinators_number-1, -1, -1)
        case _:
            print(f"[ETL] Wrong ring_sequence")
    ring_sequence = ring_sequence.tolist()
    new_settings["ring_sequence"] = ring_sequence

    return new_settings

settings = {}
with open(startup_setting_path, 'r') as infile:
    settings = json.load(infile)

settings = process_settings(settings, coordinators_number)

workers_etl_message = {
    'type': 'ETL',
    'variables': settings['variables']
}

coordinators_etl_message = {
    'type': 'ETL',
    'leader_id': settings['leader_id'], 
    'ring_sequence': settings['ring_sequence'],
    'ping_delay': settings['ping_delay'],
    'ping_timeout': settings['ping_timeout']
}

channel.basic_publish(exchange='workers', routing_key='', body=json.dumps(workers_etl_message))
channel.basic_publish(exchange='coordinators', routing_key='', body=json.dumps(coordinators_etl_message))

connection.close()

sys.exit(0)
