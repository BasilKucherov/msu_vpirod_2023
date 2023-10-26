import pika
import sys
import json

# RabbitMQ
RABBITMQ_HOST = 'localhost'

RABBITMQ_QUEUE_TO_WORKER_PREFIX = 'to_worker_'
RABBITMQ_WORKERS_EXCHANGE = 'workers'

STARTUP_STATE = 0
WORKING_STATE = 1

# get args
worker_id = int(sys.argv[1])
startup_setting_path = sys.argv[2]

# connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

# Queues
worker_queue_name = f'{RABBITMQ_QUEUE_TO_WORKER_PREFIX}{worker_id}'
channel.queue_declare(queue=worker_queue_name)

# helping vars
data = {}
commands_queue = {}
waiting_for_command_id = 0
worker_state = STARTUP_STATE

# helping funcs
def perform_command(command):
    global data

    operator =  command['operator']
    var_name =  command['var_name']
    number_operand =  float(command['number_operand'])

    if operator == 'add':
        data[var_name] += number_operand
    if operator == 'mul':
        data[var_name] *= number_operand

def process_commands_queue():
    global commands_queue
    global data
    global waiting_for_command_id

    while waiting_for_command_id in commands_queue.keys():
        perform_command(commands_queue[waiting_for_command_id])

        print(f"[W_{worker_id}] Performed command {waiting_for_command_id}: {data}")

        commands_queue.pop(waiting_for_command_id, None)
        waiting_for_command_id += 1


def callback(ch, method, properties, body):
    global data
    global commands_queue
    global worker_state

    message = json.loads(body.decode('utf-8'))

    if 'action' in message:
        if message['action'] == 'startup':
            data = message['data']['variables']
            print(f"[W_{worker_id}] Got initial vars: {data}")
            worker_state = WORKING_STATE
            process_commands_queue()
        elif message['action'] == 'command':
            command = message['command']
            command_id = int(message['command_id'])
            print(f"[W_{worker_id}] \t got command #{command_id}: {command}")
            commands_queue[command_id] = command

            if worker_state == WORKING_STATE:
                process_commands_queue()
        elif message['action'] == 'control':
            command = message['command']
            print(f"[W_{worker_id}] Got control message: {command}")

            if command == 'stop':
                sys.exit(0)

# consumers
channel.basic_consume(queue=worker_queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()
