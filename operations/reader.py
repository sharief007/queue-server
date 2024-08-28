import logging
import threading
from typing import Iterable

import utils

logger = logging.getLogger('persistance.reader ')

class QueueReader:
    def __init__(self, queue_name: str):
        self._lock = threading.Lock()

        queue_folder = utils.queue_folder(queue_name)
        queue_file = utils.queue_file(queue_folder)
        queue_meta_file = utils.queue_meta_file(queue_folder)

        self._resource = queue_file.open(mode='rb')
        self._meta = queue_meta_file.open(mode='rb')
        logger.info(f'Queue reader created: {queue_name}')

    def has_next(self) -> bool:
        with self._lock:
            curr_pos = self._resource.tell()
            self._resource.seek(0, 2)
            last_pos = self._resource.tell()
            self._resource.seek(-(last_pos - curr_pos), 2)
            return last_pos > curr_pos

    def get_next(self) -> Iterable:
        with self._lock:
            while (size_bytes := self._meta.read(4)):
                size = int.from_bytes(size_bytes, 'big')
                # compressed_message = self._resource.read(size)
                # message = zlib.decompress(compressed_message)
                # messages.append(message)
        # return messages
                yield size_bytes, size
    
