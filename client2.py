from concurrent.futures import ThreadPoolExecutor
from random import randint
from socket import socket, AF_INET, SOCK_STREAM
import time

from models.dataframe import message_from, read_message
from utils import random_string

def request(create, no):
    client = socket(AF_INET, SOCK_STREAM)
    client.connect(('127.0.0.1', 999))

    if create == 1:
        request = {'name': 'test'}
    elif create == 2:
        request = {'name': 'test', 'data': f'{no} Hello '+ random_string(10) }
    else:
        request = {'name': 'test'}

    client.send(int(create).to_bytes(4, 'big'))
    
    data = message_from(request)
    client.send(data.uncompressed_bytes)
    
    while True:
        try:
            message = read_message(lambda size: client.recv(size))
            print(message.content.decode('utf-8'))
        except:
            break


if __name__ == '__main__':
    pool = ThreadPoolExecutor(5)
    for i in range(1000):
        pool.submit(request, 3, i)
        time.sleep(1)

    pool.shutdown()
