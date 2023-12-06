import argparse
import os
import pika
import json

RABBITMQ_HOST = 'localhost'

RABBITMQ_QUEUE_MASTER = 'master'
RABBITMQ_QUEUE_MAPPER_PREFIX = 'mapper_'

SYS_DIR = '..'
TXT_DIR_NAME = "TXT"
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

def custom_print(string: str, color: str="GREEN", bold: bool=False) -> None:
    format_begin = "\033["
    format_end = "m"
    color = color_name_to_code(color)
    modificator = "1;" if bold else ""
    
    prefix = format_begin + modificator + color + format_end
    postfix = format_begin + color_name_to_code() + format_end

    print(f"{prefix}{console_prefix} {string}{postfix}")

def get_files_list(dir_path):
    items = os.listdir(dir_path)
    return [file for file in items if os.path.isfile(os.path.join(dir_path, file)) and '.txt' in file]

def txt_file_to_lines(file_path):
    with open(file_path, "r") as fp:
        lines = list(fp)
    
    return lines

def txt_folder_to_RAM(dir_path, channel, routing_key):
    file_name_list = get_files_list(dir_path)

    for file_name in file_name_list:
        file_path = os.path.join(dir_path, file_name)
        lines = txt_file_to_lines(file_path)

        message = {'type': 'data',
                   'lines': lines}
        channel.basic_publish(exchange='', routing_key=routing_key, body=json.dumps(message))

# Args
parser = argparse.ArgumentParser(description='splitter')
parser.add_argument('-i', '--id', type=int,
                    help='id')
args = parser.parse_args()
node_id = args.id

mapper_folder = os.path.join(SYS_DIR, f"{MAPPER_DIR_NAME_PREFIX}{node_id}")
mapper_queue = f"{RABBITMQ_QUEUE_MAPPER_PREFIX}{node_id}"
console_prefix = f"[SPL_{node_id}]"

# RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.queue_declare(queue=RABBITMQ_QUEUE_MASTER)
channel.queue_declare(queue=mapper_queue)
# Actual work
text_dir_path = os.path.join(mapper_folder, TXT_DIR_NAME)
txt_folder_to_RAM(text_dir_path, channel, mapper_queue)

# Send master that work done
custom_print("Work done.", bold=True)

work_done_message = {
    'type': 'SPLITTER',
    'status': 'DONE',
    'node_id': node_id,
}

channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_MASTER, body=json.dumps(work_done_message))

channel.close()
connection.close()