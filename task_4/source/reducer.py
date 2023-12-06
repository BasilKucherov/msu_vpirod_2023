import pika
import json
import sys
import argparse
import collections

RABBITMQ_HOST = 'localhost'

RABBITMQ_QUEUE_MASTER = 'master'
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

def custom_print(string: str, color: str="TEAL", bold: bool=False) -> None:
    format_begin = "\033["
    format_end = "m"
    color = color_name_to_code(color)
    modificator = "1;" if bold else ""
    
    prefix = format_begin + modificator + color + format_end
    postfix = format_begin + color_name_to_code() + format_end

    print(f"{prefix}{console_prefix} {string}{postfix}")

def finish_reduce():
    # Send master that work done
    work_done_message = {
        'type': 'REDUCER',
        'status': 'DONE',
        'node_id': node_id,
    }

    channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_MASTER, body=json.dumps(work_done_message))

    channel.close()
    connection.close()

    custom_print("Work done.")

    sys.exit(0)

def send_words_to_master(words_count):
    words_count = collections.OrderedDict(sorted(words_count.items()))

    message = {
        'type': 'data',
        'words_count': words_count,
        'node_id': node_id
    }
    channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_MASTER, body=json.dumps(message))

# reads all messages in queue and exits, when queue is empty
def consume_queue(queue_name):
    words_count = {}

    method_frame, properties, body = channel.basic_get(queue=queue_name, auto_ack=True)
    while method_frame:
        message = json.loads(body.decode('utf-8'))
        
        words = message['words']
        counts = message['counts']

        for word, count in zip(words, counts):
            count = sum(count)

            if word in words_count.keys():
                words_count[word] += count
            else:
                words_count[word] = count
        
        method_frame, properties, body = channel.basic_get(queue=queue_name, auto_ack=True)

    send_words_to_master(words_count)
    finish_reduce()

# Args
parser = argparse.ArgumentParser(description='reducer')
parser.add_argument('-i', '--id', type=int,
                    help='node id')
args = parser.parse_args()
node_id = args.id

reducer_queue = f"{RABBITMQ_QUEUE_REDUCER_PREFIX}{node_id}"
console_prefix = f"[RED_{node_id}]"

# RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.queue_declare(queue=RABBITMQ_QUEUE_MASTER)
channel.queue_declare(queue=reducer_queue)

# Actual work
consume_queue(reducer_queue)