'''
Процесс ETL (extract, transform, load) – считывает из файла geojson данные, производит
разрезание, распределяет части дорог между процессами-хранителями, сообщает менеджеру
необходимую метаинформацию.
'''
import sys
import json
from math import floor
import pika

# RabbitMQ

# RabbitMQ
RABBITMQ_HOST = 'localhost'

RABBITMQ_QUEUE_ETL_TO_MANAGER = 'etl_to_manager'
RABBITMQ_QUEUE_TO_WORKER_PREFIX = 'to_worker_'

# get args
file_path = sys.argv[1]
workers_number = int(sys.argv[2])

# connection to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

# Queues
channel.queue_declare(queue=RABBITMQ_QUEUE_ETL_TO_MANAGER)

for worker_id in range(workers_number):
    channel.queue_declare(queue=f'{RABBITMQ_QUEUE_TO_WORKER_PREFIX}{worker_id}')


# functions
def get_closest_factors(n):
    d1 = int(n ** 0.5)
    while d1 > 1:
        if n % d1 == 0:
            break

        d1 -= 1

    return [d1, n // d1]


def distribute_coordinetes(grid_size, boundaries, coordinates):
    worker_to_coordinates = {}
    worker_to_order = {}

    min_x, max_x = boundaries[0]
    min_y, max_y = boundaries[1]

    num_workers_x, num_workers_y = grid_size

    cell_width = abs(max_x - min_x) / num_workers_x
    cell_height = abs(max_y - min_y) / num_workers_y
    
    for i, (x, y) in enumerate(coordinates):
        worker_x = min(floor((x - min_x) / cell_width), num_workers_x - 1)
        worker_y = min(floor((y - min_y) / cell_height), num_workers_y - 1)
        
        worker_id = worker_y * num_workers_x + worker_x

        if worker_id in worker_to_coordinates.keys():
            worker_to_coordinates[worker_id].append([x, y])
            worker_to_order[worker_id].append(i)
        else:
            worker_to_coordinates[worker_id] = [[x, y]]
            worker_to_order[worker_id] = [i]
    
    return worker_to_coordinates, worker_to_order


# fetch data from json string
grid_size = get_closest_factors(workers_number)

with open(file_path, 'r') as infile:
    row_data = json.load(infile)

features = row_data['features']

roads_id_to_name = {}
roads_id_to_coordinates = {}
roads_id_to_workers = {}
roads_number = 0

for feature in features:
    if feature['geometry']['type'] == 'LineString' and 'name' in feature['properties'].keys():
        roads_id_to_name[roads_number] = feature['properties']['name']
        roads_id_to_coordinates[roads_number] = feature['geometry']['coordinates']
        roads_number += 1

x_boundaries = [float('inf'), float('-inf')]
y_boundaries = [float('inf'), float('-inf')]

for coordinate_list in roads_id_to_coordinates.values():
    for x, y in coordinate_list:
        x_boundaries[0] = min(x_boundaries[0], x)
        x_boundaries[1] = max(x_boundaries[1], x)

        y_boundaries[0] = min(y_boundaries[0], y)
        y_boundaries[1] = max(y_boundaries[1], y)

for road_id, coordinates in roads_id_to_coordinates.items():
    worker_to_coordinates, worker_to_order = distribute_coordinetes(grid_size, [x_boundaries, y_boundaries], roads_id_to_coordinates[road_id])

    if road_id not in roads_id_to_workers.keys():
        roads_id_to_workers[road_id] = []

    for key in worker_to_coordinates.keys():
        roads_id_to_workers[road_id].append(key)
    

    for worker_id, coordinates in worker_to_coordinates.items():
        road_part = {'road_id': road_id, 'order': worker_to_order[worker_id], 'coordinates': coordinates}
        message = {'action': 'send_road_part', 'road_part': road_part}
        channel.basic_publish(exchange='', routing_key=f'{RABBITMQ_QUEUE_TO_WORKER_PREFIX}{worker_id}', body=json.dumps(message))


message = json.dumps({'action': 'etl_finish'})
for worker_id in range(workers_number):
    channel.basic_publish(exchange='', routing_key=f'{RABBITMQ_QUEUE_TO_WORKER_PREFIX}{worker_id}', body=message)

message = json.dumps({'action': 'etl_finish', 'road_id_to_name': roads_id_to_name, 'road_id_to_workers': roads_id_to_workers})
channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE_ETL_TO_MANAGER, body=message)

connection.close()