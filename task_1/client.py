'''
После загрузки данных в систему пользователь может делать запросы с помощью клиента
— консольного приложения. Виды запросов: 1) все точки дороги по её номеру в списке
менеджера, либо 2) сам список дорог. Клиент передаёт запрос менеджеру. 
'''
import pika
import json

# RabbitMQ
RABBITMQ_HOST = 'localhost'

RABBITMQ_QUEUE_MANAGER_TO_CLIENT = 'manager_to_client'
RABBITMQ_QUEUE_CLIENT_TO_MANAGER = 'client_to_manager'

# connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

# Queues
channel.queue_declare(queue=RABBITMQ_QUEUE_CLIENT_TO_MANAGER)
channel.queue_declare(queue=RABBITMQ_QUEUE_MANAGER_TO_CLIENT)

def callback(ch, method, properties, body): 
    message = json.loads(body.decode('utf-8'))

    if 'action' in message:
        if message['action'] == 'error':
            print(f"Got error message from manager: {message.get('error')}")
        elif message['action'] == 'send_road_list':
            print(f"Got road list from manager:")
            road_list = message['road_list']

            print('Road list:')
            for id, name in road_list:
                print(f'[C]\t{id}, {name}')
            print()
        elif message['action'] == 'send_road':
            if 'error' not in message:            
                print(f"Got road from manager:")
                coordinates = message['coordinates']

                for x, y in coordinates:
                    print("[")
                    print(f"{x},")
                    print(y)
                    print("],")
            else:
                print(f"Error: {message['error']}")
    
    request_message = create_request()
    channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_CLIENT_TO_MANAGER, body=json.dumps(request_message))


def create_request():
    request_type = 0
    request_message = {}

    while request_type == 0:
        r_type = input('Type request: (1 for road list, 2 for coordinates): ')
        r_type = int(r_type)
        if r_type == 1 or r_type == 2:
            request_type = r_type
        else:
            print('Wrong input')

    if request_type == 1:
        request_message = {'action': 'request_road_list'}
    elif request_type == 2:
        road_id = input('Type road id: ')
        road_id = int(road_id)
        request_message = {'action': 'request_road_by_id', 'road_id': road_id}
    
    return request_message

# consumer
channel.basic_consume(queue=RABBITMQ_QUEUE_MANAGER_TO_CLIENT, on_message_callback=callback, auto_ack=True)

request_message = create_request()
channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_CLIENT_TO_MANAGER, body=json.dumps(request_message))

channel.start_consuming()
