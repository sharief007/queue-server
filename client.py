import time
from random import randint
from selectors import DefaultSelector, EVENT_WRITE, EVENT_READ
from socket import socket, AF_INET, SOCK_STREAM

from models.dataframe import message_from, read_message
from utils import random_string

if __name__ == '__main__':
    selector = DefaultSelector()
    client = socket(AF_INET, SOCK_STREAM)
    client.connect(('127.0.0.1', 9999))

    selector.register(client, EVENT_READ | EVENT_WRITE)

    for _ in range(10):
        for key, event_mask in selector.select():
            client: socket = key.fileobj

            if event_mask & EVENT_READ:
                message = read_message(lambda size: client.recv(size))
                print(message.content.decode('utf-8'))

            if event_mask & EVENT_WRITE:
                if randint(0, 1):
                    request = {'type': 'create_queue', 'name': random_string()}
                else:
                    request = {'type': 'push_message', 'name': 'test', 'data': random_string(1000)}

                data = message_from(request)
                client.send(data.writable_uncompressed)
            time.sleep(2)