import pika
import json
import sys
import re

# RabbitMQ
RABBITMQ_HOST = 'localhost'

RABBITMQ_QUEUE_ETL_TO_MANAGER = 'etl_to_manager'
RABBITMQ_QUEUE_CLIENT_TO_MANAGER = 'client_to_manager'
RABBITMQ_QUEUE_MANAGER_TO_COMMUNICATOR = 'manager_to_communicator'

# states
STARTUP_STATE = 0
WORKING_STATE = 1

# Helping vars
manager_state = STARTUP_STATE
data = {}
available_operators = ['add', 'mul']
commands_counter = 0

# connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

# Queues
channel.queue_declare(queue=RABBITMQ_QUEUE_ETL_TO_MANAGER)
channel.queue_declare(queue=RABBITMQ_QUEUE_CLIENT_TO_MANAGER)
channel.queue_declare(queue=RABBITMQ_QUEUE_MANAGER_TO_COMMUNICATOR)

# Helping functions
def is_number(s):
    # Define a regular expression pattern to match numbers
    number_pattern = r'^[-+]?(\d+\.\d*|\.\d+|\d+)([eE][-+]?\d+)?$'
    
    # Use re.match to check if the entire string matches the pattern
    if re.match(number_pattern, s):
        return True
    else:
        return False

def parse_command(command_str):
    global data
    global available_operators

    command_list = command_str.split(' ')

    if len(command_list) != 3:
        print('[M] Wrong command format: expected \"operator var number\"')
        return False

    operator = command_list[0]
    var_name = command_list[1]
    number_operand = command_list[2]

    if operator not in available_operators:
        print(f'[M] Wrong command format: unknown operator \"{operator}\"')
        return False

    if var_name not in data.keys():
        print(f'[M] Wrong command format: unknown var \"{var_name}\"')
        return False
    
    if not is_number(number_operand):
        print(f'[M] Wrong command format: number \"{number_operand}\" must numeric')
        return False
    
    command_parsed = {
        "operator": operator,
        "var_name": var_name,
        "number_operand": number_operand
    }

    return command_parsed


def perform_command(command):
    global data

    operator =  command['operator']
    var_name =  command['var_name']
    number_operand =  float(command['number_operand'])

    if operator == 'add':
        data[var_name] += number_operand
    if operator == 'mul':
        data[var_name] *= number_operand


# callbacks
def etl_callback(ch, method, properties, body):
    global data
    global manager_state

    message = json.loads(body.decode('utf-8'))

    if "action" in message:
        if message['action'] == 'startup':
            data = message['data']['variables']
            print(f"[M] Got initial vars: {data}")
            manager_state = WORKING_STATE



def client_callback(ch, method, properties, body):
    global commands_counter
    global data
    global manager_state

    message = json.loads(body.decode('utf-8'))

    if "action" in message:
        if manager_state != WORKING_STATE:
            print(f'[M] Not ready to accept commands, skip')
            return
        
        if message['action'] == 'command':
            command = message['command']
            print(f"[M] Got command: {command}")

            parsed_command = parse_command(command)

            if parsed_command:
                perform_command(parsed_command)
                print(f"[M] Command is acceptable! Command id: {commands_counter}")
                print(f"[M] Data after command perform: {data}")
                message_for_workers = {'action': 'command', 'command': parsed_command, 'command_id': commands_counter}
                channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_MANAGER_TO_COMMUNICATOR, body=json.dumps(message_for_workers))
                commands_counter += 1
        elif message['action'] == 'control':
            command = message['command']
            print(f"[M] Got control message: {command}")

            if command == 'stop':
                channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_MANAGER_TO_COMMUNICATOR, body=json.dumps(message))
                sys.exit(0)


# consumers
channel.basic_consume(queue=RABBITMQ_QUEUE_ETL_TO_MANAGER, on_message_callback=etl_callback, auto_ack=True)
channel.basic_consume(queue=RABBITMQ_QUEUE_CLIENT_TO_MANAGER, on_message_callback=client_callback, auto_ack=True)

channel.start_consuming()
