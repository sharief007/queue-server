from enum import Enum
from typing import Optional

from models.dataframe import DataFrame, message_from

from persistance.queue import QueueManager, QueueMapper, QueueWriter


class RequestType(Enum):
    CREATE_QUEUE = 'create_queue'
    PUSH_MESSAGE = 'push_message'

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

    queue_mapper = QueueMapper()
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

    return message_from(response)


def push_message_handler(message: DataFrame) -> DataFrame:
    if 'name' not in message.dictionary:
        raise ValueError('name is required to push message')

    name = message.dictionary['name']
    queue_mapper = QueueMapper()
    response = {'success': True}

    try:
        queue_writer = queue_mapper.get_writer(name=name)
        data = message.dictionary['data']
        queue_writer.write(data)
    except Exception as ex:
        response['success'] = False
        response['reason'] = str(ex)

    return message_from(response)


def invalid_message_handler(_: DataFrame = None) -> DataFrame:
    response = {'success': False, 'reason': 'invalid message'}
    return message_from(response)

