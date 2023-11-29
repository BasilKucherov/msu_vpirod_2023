import pika
import json
import sys
import re
import os
import threading
import time

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
    GREENITALIC = '\033[3;32m'
    TEALITALIC = '\033[3;36m'
    ENDC = '\033[0m'

lock = threading.Lock()

# RabbitMQ
RABBITMQ_HOST = 'localhost'

RABBITMQ_QUEUE_TO_MANAGER = 'to_manager'
RABBITMQ_QUEUE_MANAGER_TO_COMMUNICATOR = 'manager_to_communicator'
RABBITMQ_QUEUE_TO_COORDINATOR_PREFIX = 'to_coordinator_'

# States
ETL_WAIT = 0
ETL_DONE = 1

ELECTION_NOT_PARTICIPANT = 0
ELECTION_PARTICIPANT = 1

# States
etl_state = ETL_WAIT
election_state = ELECTION_NOT_PARTICIPANT

# Get coommand line arguments
coordinator_id = int(sys.argv[1])

# Global vars
available_operators = ['add', 'mul']
commands_counter = 0

manager_console_prefix = "[M]"
coordinator_console_prefix = f"[COORD_{coordinator_id}]"
console_prefix = coordinator_console_prefix

leader_id = None
ring_sequence = None
ring_next_id = None

ping_send_time = 0
ping_answer_time = -1
ping_delay = None
ping_timeout = None

# connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

# Queues
coordinator_queue = f'{RABBITMQ_QUEUE_TO_COORDINATOR_PREFIX}{coordinator_id}'
channel.queue_declare(queue=coordinator_queue)
channel.queue_declare(queue=RABBITMQ_QUEUE_TO_MANAGER)
channel.queue_declare(queue=RABBITMQ_QUEUE_MANAGER_TO_COMMUNICATOR)

# Payload functions
def is_number(s):
    number_pattern = r'^[-+]?(\d+\.\d*|\.\d+|\d+)([eE][-+]?\d+)?$'
    
    if re.match(number_pattern, s):
        return True
    else:
        return False

def parse_command(command_str):
    global available_operators

    command_list = command_str.split(' ')

    if len(command_list) != 3:
        print(f'{bcolors.TEAL}{console_prefix} Wrong command format: expected \"operator var number\"{bcolors.ENDC}')
        return False

    operator = command_list[0]
    var_name = command_list[1]
    number_operand = command_list[2]

    if operator not in available_operators:
        print(f'{bcolors.TEAL}{console_prefix} Wrong command format: unknown operator \"{operator}\"{bcolors.ENDC}')
        return False
    
    if not is_number(number_operand):
        print(f'{bcolors.TEAL}{console_prefix} Wrong command format: number \"{number_operand}\" must be numeric{bcolors.ENDC}')
        return False
    
    command_parsed = {
        "operator": operator,
        "var_name": var_name,
        "number_operand": number_operand
    }

    return command_parsed

# Coordinators funcs
def update_ring_sequence(dead_id):
    global ring_sequence
    global ring_next_id
    global coordinator_id

    if dead_id not in ring_sequence:
        return
    
    ring_sequence.remove(dead_id)

    for i, id in enumerate(ring_sequence):
        if id == coordinator_id:
            ring_next_id = ring_sequence[(i + 1) % len(ring_sequence)]
            break

def start_leader_election(channel_election):
    global leader_id
    global commands_counter
    global coordinator_id
    global election_state
    
    with lock:
        election_state = ELECTION_PARTICIPANT

    update_ring_sequence(leader_id)
    print(f"{bcolors.RED}{console_prefix} Detected leader death - start election -> {ring_next_id}{bcolors.ENDC}")

    election_message = {'type': 'ELECTION', 'last_known_command': commands_counter, 'proposal_id': coordinator_id}
    channel_election.basic_publish(exchange='', routing_key=f'{RABBITMQ_QUEUE_TO_COORDINATOR_PREFIX}{ring_next_id}', body=json.dumps(election_message))

def send_ping():
    global ping_send_time
    global ping_answer_time
    global ping_delay
    global ping_timeout

    global election_state
    global leader_id

    my_leader = leader_id
    leader_queue = f"{RABBITMQ_QUEUE_TO_COORDINATOR_PREFIX}{my_leader}"
    print(f"{console_prefix} Start pinging {leader_id}")

    connection_ping = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel_ping = connection_ping.channel()

    channel_ping.queue_declare(leader_queue)

    ping_message = {
        "type": "PING",
        "coordinator_id": coordinator_id
    }

    refresh_time = min(0.1, ping_timeout / (1000 * 20))

    while True and (my_leader == leader_id) and (election_state == ELECTION_NOT_PARTICIPANT):
        recieved_answer = False

        ping_send_time = time.time()
        channel_ping.basic_publish(exchange='', routing_key=leader_queue, body=json.dumps(ping_message))
        print(f"{bcolors.GREEN}{console_prefix} Ping leader{bcolors.ENDC}")

        while (time.time() - ping_send_time) < (ping_timeout / 1000):
            if election_state == ELECTION_PARTICIPANT:
                print(f"{console_prefix} ELECTION STARTED, STOP PINGING")
                return
             
            if ping_answer_time > ping_send_time:
                recieved_answer = True
                break
            
            time.sleep(refresh_time)
        
        if recieved_answer:
            time.sleep(ping_delay / 1000)
        else:
            break
    
    if my_leader != leader_id:
        print(f"{console_prefix} ELECTION PASSED, DIE")
        return

    if election_state == ELECTION_PARTICIPANT:
        print(f"{console_prefix} ELECTION STARTED, STOP PINGING")
    else:
        start_leader_election(channel_ping)

def start_ping():
    t = threading.Thread(target=send_ping, daemon=True)
    t.start()

    return t

# Callbacks
def coordinator_callback(ch, method, properties, body):
    global console_prefix
    global election_state
    global etl_state
    global leader_id
    global ring_sequence
    global ring_next_id
    global ping_delay
    global ping_timeout
    global ping_answer_time
    global commands_counter

    message = json.loads(body.decode('utf-8'))

    if "type" not in message:
        print(f"{bcolors.RED}{console_prefix} ERROR: Wrong message format!{bcolors.ENDC}")
        return
    
    match message['type']:
        case "ETL":
            if etl_state == ETL_WAIT:
                leader_id = message['leader_id']
                ring_sequence = message['ring_sequence']
                ping_delay = message['ping_delay']
                ping_timeout = message['ping_timeout']

                for i, id in enumerate(ring_sequence):
                    if id == coordinator_id:
                        ring_next_id = ring_sequence[(i + 1) % len(ring_sequence)]

                if coordinator_id == leader_id:
                    console_prefix = coordinator_console_prefix + manager_console_prefix
                    print(f"{bcolors.TEAL}{console_prefix} got ETL message! I AM LEADER! PID {os.getpid()}; Leader {leader_id}; Ring next {ring_next_id}; Ring sequence {ring_sequence}{bcolors.ENDC}")

                    channel.basic_consume(queue=RABBITMQ_QUEUE_TO_MANAGER, on_message_callback=manager_callback, auto_ack=True)
                else:
                    print(f"{console_prefix} got ETL message! PID {os.getpid()}; Leader {leader_id}; Ring next {ring_next_id}; Ring sequence {ring_sequence}{bcolors.ENDC}")
                    start_ping()

                etl_state = ETL_DONE
            elif etl_state == ETL_DONE:
                print(f"{bcolors.YELLOW}{console_prefix} WARNING: got unexpected ETL message (repetition){bcolors.ENDC}")
        case "ELECTION":
            print(f"{console_prefix} Got election message: {message}")
            m_last_know_command = message['last_known_command']
            m_proposal_id = message['proposal_id']
            
            commands_counter = m_last_know_command = max(commands_counter, m_last_know_command)

            new_message = {
                "type": "ELECTION",
                "last_known_command": commands_counter,
                "proposal_id": max(m_proposal_id, coordinator_id)
            }

            if election_state == ELECTION_NOT_PARTICIPANT:
                with lock:
                    election_state = ELECTION_PARTICIPANT

                update_ring_sequence(leader_id)

                print(f"{console_prefix} new ring: {ring_sequence} Send new election message to {ring_next_id}: {new_message}")
                channel.basic_publish(exchange='', routing_key=f'{RABBITMQ_QUEUE_TO_COORDINATOR_PREFIX}{ring_next_id}', body=json.dumps(new_message))
            elif election_state == ELECTION_PARTICIPANT:
                if m_proposal_id > coordinator_id:
                    print(f"{console_prefix} Send new election message to {ring_next_id}: {new_message}")
                    channel.basic_publish(exchange='', routing_key=f'{RABBITMQ_QUEUE_TO_COORDINATOR_PREFIX}{ring_next_id}', body=json.dumps(new_message))
                elif m_proposal_id == coordinator_id:
                    new_message["type"] = "ELECTED"
                    print(f"{console_prefix} Send new ELECTED message to {ring_next_id}: {new_message}")
                    channel.basic_publish(exchange='', routing_key=f'{RABBITMQ_QUEUE_TO_COORDINATOR_PREFIX}{ring_next_id}', body=json.dumps(new_message))
                    leader_id = coordinator_id

                    console_prefix = coordinator_console_prefix + manager_console_prefix
                    channel.queue_purge(queue=f'{RABBITMQ_QUEUE_TO_COORDINATOR_PREFIX}{ring_next_id}')
                    channel.basic_consume(queue=RABBITMQ_QUEUE_TO_MANAGER, on_message_callback=manager_callback, auto_ack=True)
                else:
                    print(f"{console_prefix} Dropped election message: {message}")
        case "ELECTED":
            if election_state == ELECTION_NOT_PARTICIPANT:
                print(f"{bcolors.RED}{console_prefix} Error: got ELECTED message without election{bcolors.ENDC}")
            elif election_state == ELECTION_PARTICIPANT:
                new_leader = message['proposal_id']          
                with lock:
                    leader_id = message['proposal_id']

                print(f"{bcolors.PURPLE}{console_prefix} ELECTION END Leader is {new_leader}.{bcolors.ENDC}")
            
                with lock:
                    election_state = ELECTION_NOT_PARTICIPANT

                if new_leader != coordinator_id:
                    channel.basic_publish(exchange='', routing_key=f'{RABBITMQ_QUEUE_TO_COORDINATOR_PREFIX}{ring_next_id}', body=json.dumps(message))

                    commands_counter = message['last_known_command']
    
                    start_ping()
                else:
                    print(f"{bcolors.PURPLEBOLD}{console_prefix} ELECTION END Leader is {leader_id}. CIRCLE DONE.{bcolors.ENDC}")
        case "PING":  
            if leader_id != coordinator_id:
                return
            
            to_coordinator_id = message["coordinator_id"]
            print(f"{bcolors.TEALITALIC}{console_prefix} Got ping from {to_coordinator_id}{bcolors.ENDC}")
            pong_message = {
                "type": "PONG"
            }

            if to_coordinator_id != leader_id and to_coordinator_id != coordinator_id:
                channel.basic_publish(exchange='', routing_key=f'{RABBITMQ_QUEUE_TO_COORDINATOR_PREFIX}{to_coordinator_id}', body=json.dumps(pong_message))
        case "PONG":
            print(f"{bcolors.GREENITALIC}{console_prefix} got leader pong{bcolors.ENDC}")
            ping_answer_time = time.time()
        case "MANAGER_COMMAND_ID":
            commands_counter = message["command_id"]
            print(f"{bcolors.GREENITALIC}{console_prefix} got leader command id {commands_counter}{bcolors.ENDC}")
        case "CONTROL":
            command = message['command']
            print(f"{console_prefix} Got control message: {command}")

            if command == 'stop':
                channel.basic_publish(exchange='', routing_key=f'{RABBITMQ_QUEUE_TO_COORDINATOR_PREFIX}{ring_next_id}', body=json.dumps(message))
                channel.queue_delete(queue=coordinator_queue)
                sys.exit(0)
        case _:
            print(f"{bcolors.RED}{console_prefix} ERROR: Unknown message type: \'{message['type']}\'{bcolors.ENDC}")


def manager_callback(ch, method, properties, body):
    global commands_counter
    global console_prefix
    message = json.loads(body.decode('utf-8'))

    if "type" not in message:
        print(f"{bcolors.RED}{console_prefix} ERROR: Wrong message format!{bcolors.ENDC}")
        return
    
    match message['type']:
        case "COMMAND":
            command = message['command']
            print(f"{bcolors.TEAL}{console_prefix} got command: {command}{bcolors.ENDC}")

            parsed_command = parse_command(command)

            if parsed_command:
                print(f"{bcolors.TEAL}{console_prefix} Command is acceptable! Command id: {commands_counter}{bcolors.ENDC}")
                message_for_workers = {'type': 'COMMAND', 'command': parsed_command, 'command_id': commands_counter}
                channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_MANAGER_TO_COMMUNICATOR, body=json.dumps(message_for_workers))

                commands_counter += 1

                message_for_fault_tolerance = {'type': 'MANAGER_COMMAND_ID', 'command_id': commands_counter}
                channel.basic_publish(exchange='', routing_key=f'{RABBITMQ_QUEUE_TO_COORDINATOR_PREFIX}{ring_next_id}', body=json.dumps(message_for_fault_tolerance))
            else:
                print(f"{bcolors.TEAL}{console_prefix} Command is UNACCEPTABLE!{bcolors.ENDC}")
        case "CONTROL":
            command = message['command']
            print(f"{bcolors.TEAL}{console_prefix} Got control message: {command}{bcolors.ENDC}")

            if command == 'stop':
                channel.basic_publish(exchange='', routing_key=f'{RABBITMQ_QUEUE_TO_COORDINATOR_PREFIX}{ring_next_id}', body=json.dumps(message))
                channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_MANAGER_TO_COMMUNICATOR, body=json.dumps(message))
                channel.queue_delete(queue=coordinator_queue)
                channel.queue_delete(queue=RABBITMQ_QUEUE_TO_MANAGER)
                sys.exit(0)
        case _:
            print(f"{bcolors.RED}{console_prefix} ERROR: Unknown message type: \'{message['type']}\'{bcolors.ENDC}")


# consumers
coordinator_tag = channel.basic_consume(queue=coordinator_queue, on_message_callback=coordinator_callback, auto_ack=True)
channel.start_consuming()
