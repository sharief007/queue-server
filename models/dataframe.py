import json
import zlib
from typing import Callable, Any, Dict


class DataFrame:
    def __init__(self, content: bytes, level: int = 1):
        self.level = level
        self.content = content
        self._compressed = None
        self._decoded = None
        self._json = None

    @property
    def compressed(self) -> bytes:
        if not self._compressed:
            self._compressed = zlib.compress(self.content, level=self.level)
        return self._compressed

    @property
    def _writable_size(self) -> bytes:
        return len(self.compressed).to_bytes(4, 'big')

    @property
    def writable_data(self) -> bytes:
        return self._writable_size + self.compressed

    @property
    def writable_uncompressed(self) -> bytes:
        return len(self.content).to_bytes(4, 'big') + self.content

    @property
    def decoded(self):
        if not self._decoded:
            self._decoded = self.content.decode(encoding='utf-8')
        return self._decoded

    @property
    def dictionary(self):
        if not self._json:
            self._json = json.loads(self.decoded)
        return self._json


def read_message(read_bytes: Callable) -> DataFrame:
    size = read_bytes(4)
    if len(size) < 4:
        raise EOFError("Failed to receive size")
    size = int.from_bytes(size, 'big')
    content = read_bytes(size)
    return DataFrame(content)


def message_from(data: Dict[str, Any]) -> DataFrame:
    content = json.dumps(data).encode('utf-8')
    print(content)
    return DataFrame(content)
