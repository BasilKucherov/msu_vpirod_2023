import pika
import json
import sys
import random


# RabbitMQ
RABBITMQ_HOST = 'localhost'

RABBITMQ_QUEUE_MANAGER_TO_COMMUNICATOR = 'manager_to_communicator'
RABBITMQ_WORKERS_EXCHANGE = 'workers'

# get args
simulator_delay = int(sys.argv[1])

# connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.exchange_declare(exchange=RABBITMQ_WORKERS_EXCHANGE, exchange_type='fanout')

# Queues
channel.queue_declare(queue=RABBITMQ_QUEUE_MANAGER_TO_COMMUNICATOR)

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
                random.shuffle(commands_queue)

                while len(commands_queue) > 0:
                    channel.basic_publish(exchange='workers', routing_key='', body=json.dumps(commands_queue.pop()))

            

# consumers
channel.basic_consume(queue=RABBITMQ_QUEUE_MANAGER_TO_COMMUNICATOR, on_message_callback=callback, auto_ack=True)

channel.start_consuming()
