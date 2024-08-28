import logging
import threading

from operations.manager import QueueManager
from .reader import QueueReader
from .writer import QueueWriter
import utils

logger = logging.getLogger('persistance.mapper ')

class QueueMapper:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls, *args, **kwargs)
                    # TODO: Use LRU cache instead of plain dict
                    cls._instance.writer_map = dict()
                    cls._instance.reader_map = dict()
        return cls._instance

    @utils.synchronized(_lock)
    def map_writer(self, name: str, writer: QueueWriter):
        self._map_writer(name, writer)
    
    def _map_writer(self, name: str, writer: QueueWriter):
        if name in self.writer_map:
            raise ValueError('Queue writer already exists')
        if not writer:
            raise ValueError('Queue writer is required')
        
        self.writer_map[name] = writer
        logger.info(f'queue writer mapped to queue: {name}')

    @utils.synchronized(_lock)
    def map_reader(self, name: str, reader: QueueReader):
        self._map_reader(name, reader)
    
    def _map_reader(self, name: str, reader: QueueReader):
        if name in self.reader_map:
            raise ValueError('Queue reader already exists')
        if not reader:
            raise ValueError('Queue reader is required')
        self.reader_map[name] = reader

    @utils.synchronized(_lock)
    def get_writer(self, name: str) -> QueueWriter:
        if name in self.writer_map:
            queue_writer = self.writer_map[name]
        elif not QueueManager._exists(name):
            logger.error('Queue does not exits on disk, so queue writer creation failed')
            raise Exception('queue does not exits')
        else:
            logger.info('Queue writer cache miss')
            queue_writer = QueueWriter(name)
            self._map_writer(name, queue_writer)          

        return queue_writer

    @utils.synchronized(_lock)
    def get_reader(self, name: str) -> QueueReader:
        if name not in self.reader_map:
            # TODO: check disk if exits then create new reader
            pass

        queue_reader = self.reader_map[name]
        return queue_reader
