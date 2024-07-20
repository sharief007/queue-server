import string

from random import randint, choice
from threading import Lock
from typing import Any, Callable
from pathlib import Path
from config import data_path, queue_prefix, queue_filename


def synchronized(lock: Lock = None, resolver: Callable = None) -> Any:
    if not lock and not resolver:
        raise ValueError('lock not found')
    if not lock:
        lock = resolver()

    def wrapper(func):
        def locked_wrapper(self, *args, **kwargs):
            with lock:
                return func(self, *args, **kwargs)

        return locked_wrapper

    return wrapper


def queue_folder(queue_name: str) -> Path:
    return Path(data_path).joinpath(queue_prefix + queue_name)


def queue_file(folder: Path) -> Path:
    return folder.joinpath(queue_filename)


def random_string(msg_size: int = 100) -> str:
    message_chars = string.ascii_letters + string.digits + ' '
    result = ""
    for _ in range(msg_size):
        result += choice(message_chars)
    return result
