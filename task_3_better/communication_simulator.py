import pika
import json
import sys
import random

class bcolors:
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    PURPLE = '\033[35m'
    TEAL = '\033[36m' # бирюзовый
    WHITE = '\033[37m'
    PURPLEBOLD = '\033[1;35m'
    ENDC = '\033[0m'

# RabbitMQ
RABBITMQ_HOST = 'localhost'

RABBITMQ_QUEUE_MANAGER_TO_COMMUNICATOR = 'manager_to_communicator'
RABBITMQ_QUEUE_TO_WORKER_PREFIX = 'to_worker_'

# Get command line arguments
simulator_delay = int(sys.argv[1])
workers_number = int(sys.argv[2])

# Global vars
commands_queue = []
console_prefix = "[COMM]"

# connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

# Queues
channel.queue_declare(queue=RABBITMQ_QUEUE_MANAGER_TO_COMMUNICATOR)

for worker_id in range(workers_number):
    channel.queue_declare(queue=f'{RABBITMQ_QUEUE_TO_WORKER_PREFIX}{worker_id}')

# Callback
def callback(ch, method, properties, body):
    global commands_queue

    message = json.loads(body.decode('utf-8'))

    if 'type' not in message:
        print(f"{bcolors.RED}{console_prefix} ERROR: Wrong message format!{bcolors.ENDC}")
        return
    
    match message['type']:
        case "COMMAND":
            commands_queue.append(message)
            print(f"{console_prefix} Got command, commands stored: {len(commands_queue)}")
            
            if len(commands_queue) >= simulator_delay:
                print(f"{console_prefix} Reached max commands queue len: {len(commands_queue)}, shuffle and send")
                for worker_id in range(workers_number):
                    worker_commands_shuffle = list(range(len(commands_queue)))
                    random.shuffle(worker_commands_shuffle)

                    for command_id in worker_commands_shuffle:
                        channel.basic_publish(exchange='', routing_key=f'{RABBITMQ_QUEUE_TO_WORKER_PREFIX}{worker_id}', body=json.dumps(commands_queue[command_id]))
                
                commands_queue = []
        case "CONTROL":
            command = message['command']

            print(f"{console_prefix} got control command {message['command']}")
            if command == 'stop':
                for worker_id in range(workers_number):
                    channel.basic_publish(exchange='', routing_key=f'{RABBITMQ_QUEUE_TO_WORKER_PREFIX}{worker_id}', body=json.dumps(message))
                
                channel.queue_delete(queue=RABBITMQ_QUEUE_MANAGER_TO_COMMUNICATOR)
                sys.exit(0)         

# consumers
channel.basic_consume(queue=RABBITMQ_QUEUE_MANAGER_TO_COMMUNICATOR, on_message_callback=callback, auto_ack=True)
channel.start_consuming()
