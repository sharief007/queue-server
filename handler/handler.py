from enum import Enum
from selectors import EVENT_READ, EVENT_WRITE, DefaultSelector
from socket import socket
import sys
from typing import Optional
import logging 
from traceback import print_exception

from models.dataframe import DataFrame, message_from

from operations.reader import QueueReader
from operations.manager import QueueManager
from operations.writer import QueueWriter
from operations.mapper import QueueMapper

logger = logging.getLogger(__name__)
queue_mapper = QueueMapper()

class RequestType(Enum):
    CREATE_QUEUE = 'create_queue'
    PUSH_MESSAGE = 'push_message'
    PULL_MESSAGE = 'pull_message'

    @classmethod
    def resolve(cls, message: DataFrame) -> Optional['RequestType']:
        req_type = message.dictionary.get('type')
        try:
            return RequestType(req_type)
        except ValueError:
            return None


def create_queue_handler(message: DataFrame) -> DataFrame:
    if 'name' not in message.dictionary:
        raise ValueError('name is required to create a queue')

    queue_manager = QueueManager()
    name = message.dictionary['name']
    response = {'success': True}

    try:
        queue_manager.create(name=name)
        queue_writer = QueueWriter(name)
        queue_mapper.map_writer(name, queue_writer)
        
    except Exception as ex:
        response['success'] = False
        response['reason'] = str(ex)
        
        logger.error('error occured while creating queue')
        print_exception(ex)
    return message_from(response)


def push_message_handler(request_frame: DataFrame) -> DataFrame:
    if 'name' not in request_frame.dictionary:
        logger.error('invalid request: name arugment is required')
        raise ValueError('name is required to push message')

    response = {'success': True}

    try:
        queue_name = request_frame.dictionary['name']
        message: str = request_frame.dictionary['data']

        queue_writer = queue_mapper.get_writer(name=queue_name)

        logger.info('writing contents to queue %s', queue_name)
        queue_writer.write(message)
        logger.info('queue write successful')
    except Exception as ex:
        response['success'] = False
        response['reason'] = str(ex)

        print_exception(ex)
    return message_from(response)

def pull_message_handler(request_frame: DataFrame, client: socket) -> DataFrame:
    response = {'success': True}

    selector = DefaultSelector()
    selector.register(client, EVENT_WRITE)

    queue_name = request_frame.dictionary['name']
    queue_reader = QueueReader(queue_name)

    offset = 0
    for size_bytes, size in queue_reader.get_next():
        _ = selector.select()
        client.send(size_bytes)
        client.sendfile(queue_reader._resource, offset, size)
        offset += size

    selector.unregister(client)
    return message_from(response)

def invalid_message_handler(_: DataFrame = None) -> DataFrame:
    response = {'success': False, 'reason': 'invalid message'}
    return message_from(response)

