import pika
import sys
import json
import re

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
RABBITMQ_QUEUE_TO_WORKER_PREFIX = 'to_worker_'

# States
ETL_WAIT = 0
ETL_DONE = 1

# Get command line arguments
worker_id = int(sys.argv[1])

# Global vars
etl_state = ETL_WAIT
commands_counter = 0

console_prefix = f"[W_{worker_id}]"

data = {}
available_operators = ['add', 'mul']
commands_queue = {}
waiting_for_command_id = 0

# connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

# Queues
worker_queue = f'{RABBITMQ_QUEUE_TO_WORKER_PREFIX}{worker_id}'
channel.queue_declare(queue=worker_queue)

# Payload functions
def is_number(s):
    number_pattern = r'^[-+]?(\d+\.\d*|\.\d+|\d+)([eE][-+]?\d+)?$'
    
    if re.match(number_pattern, s):
        return True
    else:
        return False

def perform_command(command):
    global data

    operator =  command["operator"]
    var_name =  command["var_name"]
    number_operand =  float(command["number_operand"])

    if var_name not in data.keys():
        print(f"{bcolors.RED}{console_prefix}Unknown var: {var_name}{bcolors.ENDC}")
        return

    if operator == "add":
        data[var_name] += number_operand
    if operator == "mul":
        data[var_name] *= number_operand

def process_commands_queue():
    global commands_queue
    global data
    global waiting_for_command_id

    while waiting_for_command_id in commands_queue.keys():
        perform_command(commands_queue[waiting_for_command_id])

        print(f"{console_prefix} Performed command {waiting_for_command_id}: {data}")

        commands_queue.pop(waiting_for_command_id, None)
        waiting_for_command_id += 1


# Callbacks
def worker_callback(ch, method, properties, body):
    global data
    global commands_queue
    global etl_state
    message = json.loads(body.decode('utf-8'))

    if "type" not in message:
        print(f"{bcolors.RED}{console_prefix} ERROR: Wrong message format!{bcolors.ENDC}")
        return
    
    match message['type']:
        case "ETL":
            if etl_state == ETL_WAIT:
                data = message['variables']

                print(f"{console_prefix} got ETL message. Ready to work!")
                process_commands_queue()
                etl_state = ETL_DONE
            elif etl_state == ETL_DONE:
                print(f"{bcolors.YELLOW}{console_prefix} WARNING: got unexpected ETL message (repetition){bcolors.ENDC}")
        case "COMMAND":
            command = message['command']
            command_id = int(message['command_id'])
            print(f"{console_prefix} \t got command #{command_id}: {command}")
            commands_queue[command_id] = command

            if etl_state == ETL_DONE:
                process_commands_queue()
            else:
                print(f"{bcolors.YELLOW}{console_prefix} WARNING: Got command before ETL. Drop command.{bcolors.ENDC}")
        case "CONTROL":
            command = message['command']
            print(f"{console_prefix} Got control message: {command}")

            if command == 'stop':
                channel.queue_delete(queue=worker_queue)
                sys.exit(0)
        case _:
            print(f"{bcolors.RED}{console_prefix} ERROR: Unknown message type: \'{message['type']}\'{bcolors.ENDC}")

# consumers
channel.basic_consume(queue=worker_queue, on_message_callback=worker_callback, auto_ack=True)
channel.start_consuming()
