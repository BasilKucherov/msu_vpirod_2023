import argparse
import os
import pika
import json
import re
import sys

RABBITMQ_HOST = 'localhost'

RABBITMQ_QUEUE_MASTER = 'master'
RABBITMQ_QUEUE_SHUFFLE_PREFIX = 'shuffle_'
RABBITMQ_QUEUE_MAPPER_PREFIX = 'mapper_'

MAPPER_DIR_NAME_PREFIX = "M"

def color_name_to_code(color: str="") -> str:
    match color:
        case "BLACK":
            return "30"
        case "RED":
            return "31"
        case "GREEN":
            return "32"
        case "YELLOW":
            return "33"
        case "BLUE":
            return "34"
        case "PURPLE":
            return "35"
        case "TEAL":
            return "36"
        case "WHITE":
            return "37"
        case _:
            return "0"

def custom_print(string: str, color: str="YELLOW", bold: bool=False) -> None:
    format_begin = "\033["
    format_end = "m"
    color = color_name_to_code(color)
    modificator = "1;" if bold else ""
    
    prefix = format_begin + modificator + color + format_end
    postfix = format_begin + color_name_to_code() + format_end

    print(f"{prefix}{console_prefix} {string}{postfix}")

def get_words(text):
    return re.findall(r'[a-zA-Z]+', text)

def finish_mapper():
    # Send master that work done
    work_done_message = {
        'type': 'MAPPER',
        'status': 'DONE',
        'node_id': node_id,
    }

    channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_MASTER, body=json.dumps(work_done_message))

    channel.close()
    connection.close()

    custom_print("Work done.")

    sys.exit(0)

def sort_sets_list(sets_list):
    sorted_list = sorted(sets_list, key=lambda d: d[0])
    return sorted_list    

def send_to_shuffle(words_count):
    message = {'type': 'data', 
               'data': words_count}
    channel.basic_publish(exchange='', routing_key=shuffle_queue, body=json.dumps(message))

def consume_queue(queue_name):
    lines = []
    words_count = []

    method_frame, properties, body = channel.basic_get(queue=queue_name, auto_ack=True)
    while method_frame:
        message = json.loads(body.decode('utf-8'))

        if "type" not in message:
            custom_print("ERROR: Wrong message format!", color="RED")
            return
        
        match message['type']:
            case 'data':
                lines += message['lines']
            case _:
                custom_print(f"WARNING: Unknown message type: {message['type']}", color="RED")
    
        method_frame, properties, body = channel.basic_get(queue=queue_name, auto_ack=True)

    for line in lines:
        words = get_words(line)

        for word in words:
            if not case_sesitive:
                word = word.lower()
            
            words_count.append([word, 1])
    
    words_count = sort_sets_list(words_count)
    send_to_shuffle(words_count)
    finish_mapper()

# Args
parser = argparse.ArgumentParser(description='mapper')
parser.add_argument('-i', '--id', type=int,
                    help='node id')
parser.add_argument('--case-sensitive', action=argparse.BooleanOptionalAction,
                    help='case sensitive')
args = parser.parse_args()
case_sesitive = args.case_sensitive
node_id = args.id

mapper_queue = f"{RABBITMQ_QUEUE_MAPPER_PREFIX}{node_id}"
shuffle_queue = f"{RABBITMQ_QUEUE_SHUFFLE_PREFIX}{node_id}"
console_prefix = f"[MAP_{node_id}]"

# RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.queue_declare(queue=RABBITMQ_QUEUE_MASTER)
channel.queue_declare(queue=shuffle_queue)
channel.queue_declare(queue=mapper_queue)

# Actual work
consume_queue(mapper_queue)