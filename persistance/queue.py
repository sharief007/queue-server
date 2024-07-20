import threading

import utils
from models.dataframe import DataFrame, read_message


class QueueManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    @utils.synchronized(_lock)
    def create(self, name: str, strict=True):
        if strict and self._exists(name):
            raise ValueError('Queue already exists')
        self._create(name, strict)

    @classmethod
    def _create(cls, name: str, strict: bool):
        queue_folder = utils.queue_folder(name)
        queue_folder.mkdir(exist_ok=not strict)

        queue_file = utils.queue_file(queue_folder)
        queue_file.touch(exist_ok=not strict)

    @classmethod
    def _exists(cls, name: str):
        queue_folder = utils.queue_folder(name)
        if not queue_folder.exists() or not queue_folder.is_dir():
            return False

        queue_file = utils.queue_file(queue_folder)
        if not queue_file.exists() or not queue_file.is_file():
            return False

        return True


class QueueWriter:
    def __init__(self, queue_name: str):
        self._lock = threading.Lock()
        queue_file = utils.queue_file(utils.queue_folder(queue_name))
        self._resource = queue_file.open(mode='ab')

    def write(self, message: DataFrame):
        with self._lock:
            self._resource.write(message.writable_data)
            self._resource.flush()

    def __del__(self):
        self._resource.close()


class QueueReader:
    def __init__(self, queue_name: str):
        self._lock = threading.Lock()
        self._resource = open(queue_name, mode='rb')

    def has_next(self) -> bool:
        with self._lock:
            curr_pos = self._resource.tell()
            self._resource.seek(0, 2)
            last_pos = self._resource.tell()
            self._resource.seek(-(last_pos - curr_pos), 2)
            return last_pos > curr_pos

    def get_next(self) -> DataFrame:
        with self._lock:
            return read_message(lambda size: self._resource.read(size))


class QueueMapper:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
                    # TODO: Use LRU cache instead of plain dict
                    cls._instance.writer_map = dict()
                    cls._instance.reader_map = dict()
        return cls._instance

    @utils.synchronized(_lock)
    def map_writer(self, name: str, writer: QueueWriter):
        if name in self.writer_map:
            raise ValueError('Queue writer already exists')
        if not writer:
            raise ValueError('Queue writer is required')
        self.writer_map[name] = writer

    @utils.synchronized(_lock)
    def map_reader(self, name: str, reader: QueueReader):
        if name in self.reader_map:
            raise ValueError('Queue reader already exists')
        if not reader:
            raise ValueError('Queue reader is required')
        self.reader_map[name] = reader

    @utils.synchronized(_lock)
    def get_writer(self, name: str) -> QueueWriter:
        if name not in self.writer_map:
            # TODO: check disk if exists then create new writer
            pass

        queue_writer = self.writer_map[name]
        return queue_writer

    @utils.synchronized(_lock)
    def get_reader(self, name: str) -> QueueReader:
        if name not in self.reader_map:
            # TODO: check disk if exits then create new reader
            pass

        queue_reader = self.reader_map[name]
        return queue_reader
