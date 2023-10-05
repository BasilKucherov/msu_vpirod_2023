'''
Каждый рабочий процесс отвечает за свою часть карты. На каждом из них хранятся части
дорог, расположенные в соответствующей части. 
'''
import pika
import sys
import json

# RabbitMQ
RABBITMQ_HOST = 'localhost'

RABBITMQ_QUEUE_WORKERS_TO_MANAGER = 'workers_to_manager'
RABBITMQ_QUEUE_TO_WORKER_PREFIX = 'to_worker_'

# get args
worker_id = int(sys.argv[1])

# connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

# Queues
channel.queue_declare(queue=RABBITMQ_QUEUE_WORKERS_TO_MANAGER)
channel.queue_declare(queue=f'{RABBITMQ_QUEUE_TO_WORKER_PREFIX}{worker_id}')

road_id_to_road_part = {}


def callback(ch, method, properties, body):
    global road_id_to_road_part

    message = json.loads(body.decode('utf-8'))

    if 'action' in message:
        if message['action'] == 'send_road_part':
            print(f"[W_{worker_id}] got road part")

            road_part = message.get('road_part')

            road_id = road_part['road_id']
            road_id_to_road_part[str(road_id)] = road_part
        elif message['action'] == 'etl_finish':
            print(f"[W_{worker_id}] ETL finished")
        elif message['action'] == 'request_road_part':            
            road_id = str(message['road_id'])
            job_id = message['job_id']
    
            print(f"[W_{worker_id}] got request for road {road_id}")

            response_message = {'action': 'send_data', 'road_part': road_id_to_road_part[road_id], 'job_id': job_id}
            channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_WORKERS_TO_MANAGER, body=json.dumps(response_message))

# consumers
channel.basic_consume(queue=f'{RABBITMQ_QUEUE_TO_WORKER_PREFIX}{worker_id}', on_message_callback=callback, auto_ack=True)

channel.start_consuming()
