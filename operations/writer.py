
import logging
import threading

from models.dataframe import DataFrame
import utils

logger = logging.getLogger('persistance.writer ')

class QueueWriter:
    def __init__(self, queue_name: str):
        logger.info('Initializing queue writer from disk')
        self._lock = threading.Lock()
        self.level = 1
        
        queue_folder = utils.queue_folder(queue_name)
        queue_file = utils.queue_file(queue_folder)
        queue_meta_file = utils.queue_meta_file(queue_folder)

        self._resource = queue_file.open(mode='ab')
        self._meta = queue_meta_file.open(mode='ab')
        logger.info(f'Queue writer created: {queue_name}')

    def write(self, message: str | bytes):
        frame = DataFrame(message.encode())
        with self._lock:
            self._meta.write(frame.uncompressed_size)
            self._resource.write(frame.content)

            self._resource.flush()
            self._meta.flush()

    def __del__(self):
        self._resource.close()
        self._meta.close()
