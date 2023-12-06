import os
import pika
import json
import sys
import string
import argparse

RABBITMQ_HOST = 'localhost'

RABBITMQ_QUEUE_MASTER = 'master'
RABBITMQ_QUEUE_SHUFFLE_PREFIX = 'shuffle_'
RABBITMQ_QUEUE_REDUCER_PREFIX = 'reducer_'

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

def custom_print(string: str, color: str="BLUE", bold: bool=False) -> None:
    format_begin = "\033["
    format_end = "m"
    color = color_name_to_code(color)
    modificator = "1;" if bold else ""
    
    prefix = format_begin + modificator + color + format_end
    postfix = format_begin + color_name_to_code() + format_end

    print(f"{prefix}{console_prefix} {string}{postfix}")

def finish_shuffle():
    # Send master that work done
    work_done_message = {
        'type': 'SHUFFLE',
        'status': 'DONE',
        'node_id': node_id,
    }

    channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_MASTER, body=json.dumps(work_done_message))

    channel.close()
    connection.close()

    custom_print("Work done.")

    sys.exit(0)

def get_letter_to_reducer():
    if case_sensitive:
        available_letters = list(string.ascii_letters)
    else:
        available_letters = list(string.ascii_lowercase)
    
    letters_per_reducer = len(available_letters) // reducers_number
    residue = len(available_letters) % reducers_number

    letter_to_reducer = {}
    letter_idx = 0
    for i in range(0, reducers_number):
        for j in range(letters_per_reducer + (1 if residue > 0 else 0)):
            letter_to_reducer[available_letters[letter_idx]] = i
            letter_idx += 1

        residue -= 1
    
    return letter_to_reducer


def distribute_words_to_reducers(words_count):
    letter_to_reducer = get_letter_to_reducer()

    reducer_message_data = {}
    for word, counts in words_count.items():
        message = {'word': word, 'counts': counts}

        reducer_id = letter_to_reducer[word[0]]

        if reducer_id in reducer_message_data.keys():
            reducer_message_data[reducer_id]['words'].append(word)
            reducer_message_data[reducer_id]['counts'].append(counts)
        else:
            reducer_message_data[reducer_id] = {
                'words': [word],
                'counts': [counts],
            }
        
    for reducer_id, data in reducer_message_data.items():
        reducer_queue = f"{RABBITMQ_QUEUE_REDUCER_PREFIX}{reducer_id}"
        channel.basic_publish(exchange='', routing_key=reducer_queue, body=json.dumps(data))

# reads all messages in queue and exits, when queue is empty
def consume_queue(queue_name):
    words_count_list = []
    words_count = {}

    method_frame, properties, body = channel.basic_get(queue=queue_name, auto_ack=True)
    while method_frame:
        message = json.loads(body.decode('utf-8'))
        
        if "type" not in message:
            custom_print("ERROR: Wrong message format!", color="RED")
            return
        
        match message['type']:
            case 'data':
                words_count_list += message['data']
            case _:
                custom_print(f"WARNING: Unknown message type: {message['type']}", color="RED")
        
        method_frame, properties, body = channel.basic_get(queue=queue_name, auto_ack=True)

    for word, count in words_count_list:
        if not case_sensitive:
            word = word.lower()

        if word in words_count.keys():
            words_count[word].append(count)
        else:
            words_count[word] = [count]

    distribute_words_to_reducers(words_count)
    finish_shuffle()


# Get command line argumnets
parser = argparse.ArgumentParser(description='Create map-reduce system structure')
parser.add_argument('-r', '--reducers-number', type=int,
                    help='number of reducers')
parser.add_argument('-i', '--id', type=int,
                    help='node id')
parser.add_argument('--case-sensitive', action=argparse.BooleanOptionalAction,
                    help='case sensitive')
args = parser.parse_args()

reducers_number = args.reducers_number
node_id = args.id
case_sensitive = bool(args.case_sensitive)

# Determine node id (Could be sent by master, but i did it like that)
shuffle_queue = f"{RABBITMQ_QUEUE_SHUFFLE_PREFIX}{node_id}"
console_prefix = f"[SHF_{node_id}]"

# RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.queue_declare(queue=RABBITMQ_QUEUE_MASTER)
channel.queue_declare(queue=shuffle_queue)

for i in range(reducers_number):
    channel.queue_declare(queue=f"{RABBITMQ_QUEUE_REDUCER_PREFIX}{i}")

# Actual work
consume_queue(shuffle_queue)