import logging
import threading

import utils

logger = logging.getLogger('persistance.manager ')

class QueueManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    @utils.synchronized(_lock)
    def create(self, name: str, strict=True):
        if strict and self._exists(name):
            logging.error('Queue already exists')
            raise ValueError('Queue already exists')
        
        logger.info(f'Creating new queue: {name}')
        self._create(name, strict)
        logger.info(f'Queue {name} created successfully')

    @classmethod
    def _create(cls, name: str, strict: bool):
        queue_folder = utils.queue_folder(name)
        queue_folder.mkdir(exist_ok=not strict)

        queue_file = utils.queue_file(queue_folder)
        queue_file.touch(exist_ok=not strict)

        meta_file = utils.queue_meta_file(queue_folder)
        meta_file.touch(exist_ok=not strict)

    @classmethod
    def _exists(cls, name: str):
        queue_folder = utils.queue_folder(name)
        if not queue_folder.exists() or not queue_folder.is_dir():
            return False

        queue_file = utils.queue_file(queue_folder)
        if not queue_file.exists() or not queue_file.is_file():
            return False

        meta_file = utils.queue_meta_file(queue_folder)
        if not meta_file.exists() or not meta_file.is_file():
            return False
        
        return True