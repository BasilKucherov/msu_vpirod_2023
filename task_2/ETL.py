'''
Процесс ETL (extract, transform, load) – load vars from file and send to workers and manager
'''
import sys
import json
import pika

# RabbitMQ
RABBITMQ_HOST = 'localhost'

RABBITMQ_QUEUE_ETL_TO_MANAGER = 'etl_to_manager'
RABBITMQ_QUEUE_TO_WORKER_PREFIX = 'to_worker_'

# get args
workers_number = int(sys.argv[1])
startup_setting_path = sys.argv[2]

# connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.exchange_declare(exchange='workers', exchange_type='fanout')

# Queues
channel.queue_declare(queue=RABBITMQ_QUEUE_ETL_TO_MANAGER)

for worker_id in range(workers_number):
    queue_name = f'{RABBITMQ_QUEUE_TO_WORKER_PREFIX}{worker_id}'
    channel.queue_declare(queue=queue_name)
    channel.queue_bind(exchange='workers', queue=queue_name)


# functions
data = {}
with open(startup_setting_path, 'r') as infile:
    data = json.load(infile)

setup_message = {'action': 'startup', 'data': data}
channel.basic_publish(exchange='workers', routing_key='', body=json.dumps(setup_message))
channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_ETL_TO_MANAGER, body=json.dumps(setup_message))

connection.close()

sys.exit(0)
