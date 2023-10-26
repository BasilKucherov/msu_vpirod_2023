import pika
import json
import sys
import random
import copy


# RabbitMQ
RABBITMQ_HOST = 'localhost'

RABBITMQ_QUEUE_MANAGER_TO_COMMUNICATOR = 'manager_to_communicator'
RABBITMQ_QUEUE_TO_WORKER_PREFIX = 'to_worker_'


# get args
simulator_delay = int(sys.argv[1])
workers_number = int(sys.argv[2])

# connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

# Queues
channel.queue_declare(queue=RABBITMQ_QUEUE_MANAGER_TO_COMMUNICATOR)

for worker_id in range(workers_number):
    channel.queue_declare(queue=f'{RABBITMQ_QUEUE_TO_WORKER_PREFIX}{worker_id}')


# helping vars
commands_queue = []

# helping funcs
def callback(ch, method, properties, body):
    global commands_queue

    message = json.loads(body.decode('utf-8'))

    if 'action' in message:
        if message['action'] == 'command':
            commands_queue.append(message)
            print(f"[COMM] Got command, commands stored: {len(commands_queue)}")

            if len(commands_queue) >= simulator_delay:
                print(f"[COMM] Reached max commands queue len: {len(commands_queue)}, shuffle and send")
                for worker_id in range(workers_number):
                    worker_commands_shuffle = list(range(len(commands_queue)))
                    random.shuffle(worker_commands_shuffle)

                    for command_id in worker_commands_shuffle:
                        channel.basic_publish(exchange='', routing_key=f'{RABBITMQ_QUEUE_TO_WORKER_PREFIX}{worker_id}', body=json.dumps(commands_queue[command_id]))
                
                commands_queue = []
        elif message['action'] == 'control':
            command = message['command']

            print(f"[COMM] got control command {message['command']}")
            if command == 'stop':
                for worker_id in range(workers_number):
                    channel.basic_publish(exchange='', routing_key=f'{RABBITMQ_QUEUE_TO_WORKER_PREFIX}{worker_id}', body=json.dumps(message))
                sys.exit(0)
                            

# consumers
channel.basic_consume(queue=RABBITMQ_QUEUE_MANAGER_TO_COMMUNICATOR, on_message_callback=callback, auto_ack=True)

channel.start_consuming()
