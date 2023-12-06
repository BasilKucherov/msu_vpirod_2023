import argparse
import os
import shutil
import subprocess
import pika
import json
import sys

# Constants
RABBITMQ_HOST = 'localhost'
RABBITMQ_QUEUE_MASTER = 'master'

MASTER_DIR_NAME = 'T'
MAPPER_DIR_NAME_PREFIX = 'M'
REDUCER_DIR_NAME_PREFIX = 'R'

SYSTEM_DIR = '..'

MASTER_NAME = "master.py"
MAPPER_NAME = "mapper.py"
REDUCER_NAME = "reducer.py"
SHUFFLE_NAME = "shuffle.py"
SPLITTER_NAME = "splitter.py"

TXT_DIR_NAME = "TXT"

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

def custom_print(string: str, color: str="PURPLE", bold: bool=False) -> None:
    format_begin = "\033["
    format_end = "m"
    color = color_name_to_code(color)
    modificator = "1;" if bold else ""
    
    prefix = format_begin + modificator + color + format_end
    postfix = format_begin + color_name_to_code() + format_end

    print(f"{prefix}{console_prefix} {string}{postfix}")

def get_mappers(mappers_number: int) -> dict:
    mapper_id_to_dir = {}
    
    for mapper_id in range(mappers_number):
        mapper_dir = os.path.join(SYSTEM_DIR, MAPPER_DIR_NAME_PREFIX + str(mapper_id))
        mapper_id_to_dir[mapper_id] = mapper_dir
        
        if not os.path.isdir(mapper_dir):
            custom_print(f"ERROR: There is no mapper with id {mapper_id}", color="RED", bold=True)
            sys.exit(1)

    return mapper_id_to_dir

def get_reducers(reducers_number: int) -> dict:
    reducer_id_to_dir = {}
    
    for reducer_id in range(reducers_number):
        reducer_dir = os.path.join(SYSTEM_DIR, REDUCER_DIR_NAME_PREFIX + str(reducer_id))
        reducer_id_to_dir[reducer_id] = reducer_dir

        if not os.path.isdir(reducer_dir):
            custom_print(f"ERROR: There is no reducer with id {reducer_id}", color="RED", bold=True)
            sys.exit(1)

    return reducer_id_to_dir

def distribute_texts(texts_dir_path: str, mappers: dict) -> None:
    items = os.listdir(texts_dir_path)
    texts = [i for i in items if os.path.isfile(os.path.join(texts_dir_path, i)) and i.endswith('.txt')]

    texts_number = len(texts)
    mappers_number = len(mappers.keys())

    texts_per_mapper = texts_number // mappers_number
    residue = texts_number % mappers_number

    texts_idx = 0

    custom_print(f"texts per mapper = {texts_per_mapper}")
    custom_print(f"residue = {residue}")
    for mapper_id, mapper_dir in mappers.items():       
        mapper_text_dir = os.path.join(mapper_dir, TXT_DIR_NAME)
        os.makedirs(mapper_text_dir, exist_ok=True)

        for j in range(texts_per_mapper + (1 if residue > 0 else 0)):
            text_path = os.path.join(texts_dir_path, texts[texts_idx])
            custom_print(f"\t{text_path} => {mapper_text_dir}")
            shutil.copy(text_path, mapper_text_dir)
            texts_idx += 1

        residue -= 1

def delete_texts(mappers: dict) -> None:
    for mapper_id, mapper_dir in mappers.items():
        mapper_text_dir = os.path.join(mapper_dir, TXT_DIR_NAME)
        custom_print(f"Deleting {mapper_text_dir}")
        if os.path.exists(mapper_text_dir) and os.path.isdir(mapper_text_dir):
            shutil.rmtree(mapper_text_dir)

def run_splitters(mappers: dict) -> None:
    global wait_list_splitters
    for mapper_id, mapper_dir in mappers.items():
        wait_list_splitters.append(mapper_id)
        splitter = os.path.join(mapper_dir, SPLITTER_NAME)
        subprocess.Popen(['python3', splitter, '-i', str(mapper_id)])

def run_mappers(mappers: dict):
    global wait_list_mappers

    for mapper_id, mapper_dir in mappers.items():
        wait_list_mappers.append(mapper_id)
        mapper = os.path.join(mapper_dir, MAPPER_NAME)
        case_flag = "--case-sensitive" if case_sensitive else "--no-case-sensitive"

        subprocess.Popen(['python3', mapper, '-i', str(mapper_id), case_flag])

def run_shufflers(mappers: dict):
    global wait_list_shufflers

    for mapper_id, mapper_dir in mappers.items():
        wait_list_shufflers.append(mapper_id)
        shuffle = os.path.join(mapper_dir, SHUFFLE_NAME)

        if case_sensitive:
            cs_arg = '--case-sensitive'
        else:
            cs_arg = '--no-case-sensitive'

        subprocess.Popen(['python3', shuffle, cs_arg, '-r', str(reducers_number), '-i', str(mapper_id)])

def run_reducers(reducers: dict):
    global wait_list_reducers

    for reducer_id, reducer_dir in reducers.items():
        wait_list_reducers.append(reducer_id)
        reducer = os.path.join(reducer_dir, REDUCER_NAME)
        subprocess.Popen(['python3', reducer, '-i', str(reducer_id)])

def show_result(reducers: dict):
    reducers_ids = sorted(reducers.keys())
    word_count = {}
    for id in reducers_ids:
        word_count = dict(list(word_count.items()) + list(reducers_data[id].items()))
    
    custom_print("RESULT")
    for word, count in word_count.items():
        custom_print(f"{word:20s}: {count}", color="WHITE")
    
    with open('word_count.json', 'w') as fp:
        json.dump(word_count, fp)
    
def finish_master():
    custom_print("Work done.")
    sys.exit(0)

def callback(ch, method, properties, body):
    global wait_list_splitters
    global wait_list_shufflers
    global wait_list_mappers
    global wait_list_reducers
    global word_count
    global reducers_data

    message = json.loads(body.decode('utf-8'))

    if "type" not in message:
        custom_print(f"WARNING: Wrong message format!", color="RED", bold=True)
        return

    match message['type']:
        case 'SPLITTER':
            if message['status'] == 'DONE':
                wait_list_splitters.remove(int(message['node_id']))
            
            if not wait_list_splitters:
                custom_print("Splitters done.")
                run_mappers(mappers)
        case 'MAPPER':
            if message['status'] == 'DONE':
                wait_list_mappers.remove(int(message['node_id']))
            
            if not wait_list_mappers:
                custom_print("Mappers done.")
                run_shufflers(mappers)
        case 'SHUFFLE':
            if message['status'] == 'DONE':
                wait_list_shufflers.remove(int(message['node_id']))
            
            if not wait_list_shufflers:
                custom_print("Shufflers done.")
                run_reducers(reducers)
        case 'REDUCER':
            if message['status'] == 'DONE':
                wait_list_reducers.remove(int(message['node_id']))
            
            if not wait_list_reducers:
                custom_print("Reducers done.")
                show_result(reducers)
                delete_texts(mappers)

                sys.exit(0)
        case 'data':
            id = message['node_id']
            reducers_data[id] = message['words_count']
        case _:
            custom_print(f"WARNING: Unknown message type: {message['type']}", color="RED")

# argparser
def check_positive(numeric_type):
    def require_positive(value):
        number = numeric_type(value)
        if number <= 0:
            raise argparse.ArgumentTypeError(f"Number {value} must be positive.")
        return number

    return require_positive

def check_folder_path(path):
    def path_exists(path):
        if not os.path.isdir(path):
            raise argparse.ArgumentTypeError(f"string \'{path}\' must be existing file path.")
        return path

    return path_exists

parser = argparse.ArgumentParser(description='Run Map-Reduce')
parser.add_argument('-k', '--reducers-number', type=check_positive(int), default=1,
                    help='number of reducers')
parser.add_argument('-n', '--mappers-number', type=check_positive(int), default=1,
                    help='number of mappers')
parser.add_argument('-p', '--texts-path', type=check_folder_path(str), default=TXT_DIR_NAME,
                    help='path to texts folder')
parser.add_argument('--case-sensitive', action=argparse.BooleanOptionalAction,
                    help='case senitive')
args = parser.parse_args()

case_sensitive = bool(args.case_sensitive)
mappers_number = args.mappers_number
reducers_number = args.reducers_number
texts_dir_path = args.texts_path

# vars
console_prefix = "[MASTER]"

wait_list_splitters = []
wait_list_mappers = []
wait_list_shufflers = []
wait_list_reducers = []

reducers_data = {}
word_count = {}

# RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.queue_declare(queue=RABBITMQ_QUEUE_MASTER)
channel.basic_consume(queue=RABBITMQ_QUEUE_MASTER, on_message_callback=callback, auto_ack=True)

# actual work
mappers = get_mappers(mappers_number)
reducers = get_reducers(reducers_number)

print(f"mappers = {mappers}")
print(f"reducers = {reducers}")

distribute_texts(texts_dir_path, mappers)

run_splitters(mappers)
channel.start_consuming()
