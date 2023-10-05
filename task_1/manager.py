'''
Менеджер хранит информацию о том, где какие данные хранятся. Хранит список: номер
дороги, имя дороги. Имя дороги — это значение поле name из блока properties. Номер
присваивается при загрузке. Менеджер понимает два вида запросов: прислать список дорог,
прислать все точки дороги с заданным номеров. 
'''
import pika
import json
import uuid

# RabbitMQ
RABBITMQ_HOST = 'localhost'

RABBITMQ_QUEUE_ETL_TO_MANAGER = 'etl_to_manager'
RABBITMQ_QUEUE_WORKERS_TO_MANAGER = 'workers_to_manager'
RABBITMQ_QUEUE_CLIENT_TO_MANAGER = 'client_to_manager'
RABBITMQ_QUEUE_TO_WORKER_PREFIX = 'to_worker_'
RABBITMQ_QUEUE_MANAGER_TO_CLIENT = 'manager_to_client'

# State of the service
INITIAL_STATE = 0
READY_STATE = 1 
ETL_STATE = 2

service_state = INITIAL_STATE
road_id_to_workers_ids = {}
road_id_to_name = {}
road_id_name = []

jobs = {}

parts_recieved = 0
road_coordinates = []
road_order = []

# connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

# Queues
channel.queue_declare(queue=RABBITMQ_QUEUE_ETL_TO_MANAGER)
channel.queue_declare(queue=RABBITMQ_QUEUE_WORKERS_TO_MANAGER)
channel.queue_declare(queue=RABBITMQ_QUEUE_CLIENT_TO_MANAGER)
channel.queue_declare(queue=RABBITMQ_QUEUE_MANAGER_TO_CLIENT)

# callbacks
def etl_callback(ch, method, properties, body):
    global service_state
    global road_id_to_name
    global road_id_to_workers_ids
    global road_id_name

    message = json.loads(body.decode('utf-8'))

    if "action" in message:
        if message['action'] == 'etl_start':
            print("[M] ETL started")
            service_state = ETL_STATE
        elif message['action'] == 'etl_finish':
            print("[M] ETL finished")
            service_state = READY_STATE
            road_id_to_name = message['road_id_to_name']
            road_id_to_workers_ids = message['road_id_to_workers']
            road_id_name = [[road_id, road_name] for road_id, road_name in road_id_to_name.items()]

def client_callback(ch, method, properties, body):
    message = json.loads(body.decode('utf-8'))

    if "action" in message:
        if service_state != READY_STATE:                
            print("[M] Client requested while seervice not ready")

            response_message = {'action': 'error',
                                 'error': 'service is not ready, please wait'}
            channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_MANAGER_TO_CLIENT, body=json.dumps(response_message))
        else:
            if message['action'] == 'request_road_list':
                print("[M] Client requested available road list")

                response_message = {'action': 'send_road_list',
                                    'road_list': road_id_name}
                channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_MANAGER_TO_CLIENT, body=json.dumps(response_message))

            elif message['action'] == 'request_road_by_id': 
                road_id = str(message.get('road_id', None))
                print(f"[M] Client requested road with id {road_id}")

                if road_id in road_id_to_name:
                    request_road_from_workers(road_id)       
                else:
                    response_message = {'action': 'send_road',
                                        'error': f'No such road with id {road_id}'}
                    channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_MANAGER_TO_CLIENT, body=json.dumps(response_message))   


def workers_callback(ch, method, properties, body):    
    global jobs

    message = json.loads(body.decode('utf-8'))

    if 'action' in message:
        if message['action'] == 'send_data':
            job_id = message['job_id']
            print(f"[M] Got data from worker for job {job_id}")

            jobs[job_id]['parts_left'] -= 1
            jobs[job_id]['coordinates'] += message['road_part']['coordinates']
            jobs[job_id]['order'] += message['road_part']['order']

            if jobs[job_id]['parts_left'] <= 0:
                print(f'[M] Got all data for job {job_id}, sending to client')
                coordinates = build_road(jobs[job_id]['coordinates'], jobs[job_id]['order'])
                response_message = {'action': 'send_road', 'coordinates': coordinates}
                channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_MANAGER_TO_CLIENT, body=json.dumps(response_message))
                jobs.pop(job_id, None)



# other functions
def request_road_from_workers(road_id):
    global jobs
    global road_id_to_workers_ids

    road_id = str(road_id)
    
    job_id = str(uuid.uuid4())
    jobs[job_id] = {'coordinates': [], 'order': [], 'parts_left': len(road_id_to_workers_ids[road_id])}

    request_message = {'action': 'request_road_part', 'road_id': road_id, 'job_id': job_id}
    for worker_id in road_id_to_workers_ids[road_id]:
        channel.basic_publish(exchange='', routing_key=f'{RABBITMQ_QUEUE_TO_WORKER_PREFIX}{worker_id}', body=json.dumps(request_message))


def build_road(coordinates, order):
    combined = list(zip(order, coordinates))
    combined.sort(key=lambda x: x[0])
    coordinates_sorted = [x[1] for x in combined]

    return coordinates_sorted


# consumers
channel.basic_consume(queue=RABBITMQ_QUEUE_ETL_TO_MANAGER, on_message_callback=etl_callback, auto_ack=True)
channel.basic_consume(queue=RABBITMQ_QUEUE_CLIENT_TO_MANAGER, on_message_callback=client_callback, auto_ack=True)
channel.basic_consume(queue=RABBITMQ_QUEUE_WORKERS_TO_MANAGER, on_message_callback=workers_callback, auto_ack=True)

channel.start_consuming()
