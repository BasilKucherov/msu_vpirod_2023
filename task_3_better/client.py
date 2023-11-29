import pika
import json

# RabbitMQ
RABBITMQ_HOST = 'localhost'
RABBITMQ_QUEUE_TO_MANAGER = 'to_manager'

# connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

# Queues
channel.queue_declare(queue=RABBITMQ_QUEUE_TO_MANAGER)
    
def create_request():
    request_message = {}

    command_str = input('[C] Type command: ')

    if command_str == 'stop':
        request_message['type'] = 'CONTROL'  
    else:
        request_message['type'] = 'COMMAND'  
    
    request_message['command'] = command_str
      
    return request_message


# consumer
while True:
    request_message = create_request()
    channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_TO_MANAGER, body=json.dumps(request_message))
