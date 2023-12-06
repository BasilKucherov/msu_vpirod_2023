import argparse
import os
import shutil
from distutils.dir_util import copy_tree

# Constants
SOURCES_PATH = 'source'

MASTER_DIR_NAME = 'T'
MAPPER_DIR_NAME_PREFIX = 'M'
REDUCER_DIR_NAME_PREFIX = 'R'

MASTER_NAME = "master.py"
MAPPER_NAME = "mapper.py"
REDUCER_NAME = "reducer.py"
SHUFFLE_NAME = "shuffle.py"
SPLITTER_NAME = "splitter.py"

TXT_DIR_PATH = "TXT"
TXT_DIR_NAME = "TXT"

# argparser
def check_positive(numeric_type):
    def require_positive(value):
        number = numeric_type(value)
        if number <= 0:
            raise argparse.ArgumentTypeError(f"Number {value} must be positive.")
        return number

    return require_positive


parser = argparse.ArgumentParser(description='Create map-reduce system structure')
parser.add_argument('-r', '--reducers-number', type=check_positive(int), default=1,
                    help='number of reducers')
parser.add_argument('-m', '--mappers-number', type=check_positive(int), default=1, 
                    help='number of mappers (map, split, shuffle)')
parser.add_argument('-p', '--destination-path', type=str, default="system",
                    help='DIR path, where system will be created')


args = parser.parse_args()

# MASTER
master_dir = os.path.join(args.destination_path, MASTER_DIR_NAME)
os.makedirs(master_dir, exist_ok=True)
shutil.copy(os.path.join(SOURCES_PATH, MASTER_NAME), master_dir)
copy_tree(TXT_DIR_PATH, os.path.join(master_dir, TXT_DIR_NAME))

# MAPPERs
for i in range(0, args.mappers_number):
    mapper_dir = os.path.join(args.destination_path, f"{MAPPER_DIR_NAME_PREFIX}{i}")
    os.makedirs(mapper_dir, exist_ok=True)
    
    shutil.copy(os.path.join(SOURCES_PATH, MAPPER_NAME), mapper_dir)
    shutil.copy(os.path.join(SOURCES_PATH, SHUFFLE_NAME), mapper_dir)
    shutil.copy(os.path.join(SOURCES_PATH, SPLITTER_NAME), mapper_dir)

# REDUCERs
for i in range(0, args.reducers_number):
    reducer_dir = os.path.join(args.destination_path, f"{REDUCER_DIR_NAME_PREFIX}{i}")
    os.makedirs(reducer_dir, exist_ok=True)

    shutil.copy(os.path.join(SOURCES_PATH, REDUCER_NAME), reducer_dir)

