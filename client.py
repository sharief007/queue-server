from concurrent.futures import ThreadPoolExecutor
from random import randint
from socket import socket, AF_INET, SOCK_STREAM

from models.dataframe import message_from, read_message
from utils import random_string

def request(create):
    client = socket(AF_INET, SOCK_STREAM)
    client.connect(('127.0.0.1', 9999))

    if create == 1:
        request = {'type': 'create_queue', 'name': 'test'}
    elif create == 2:
        request = {'type': 'push_message', 'name': 'test', 'data': 'Hello '+ random_string(10)}
    else:
        request = {'type': 'pull_message', 'name': 'test'}

    data = message_from(request)
    print('message type: ', request['type'])
    client.send(data.uncompressed_bytes)
    
    if create == 3:
        while True:
            try:
                message = read_message(lambda size: client.recv(size))
                # message = message.decompressed
                print(message.content.decode('utf-8'))
            except:
                break
    else:
        message = read_message(lambda size: client.recv(size))
        print(message.content.decode('utf-8'))


if __name__ == '__main__':
    pool = ThreadPoolExecutor(5)
    for _ in range(1):
        pool.submit(request, 2)

    pool.shutdown()
