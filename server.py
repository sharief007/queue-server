from collections.abc import Callable
import logging
from socketserver import BaseRequestHandler, ThreadingTCPServer
from typing import Any

from handler.handler import create_queue_handler, pull_message_handler, push_message_handler, RequestType, invalid_message_handler
from models.dataframe import read_message

logger = logging.getLogger(__name__)

class RequestHandler(BaseRequestHandler):
    def __init__(self, request, client_address, server: ThreadingTCPServer):
        super().__init__(request, client_address, server)

    def handle(self):
        request_frame = read_message(lambda size: self.request.recv(size))
        msg_type = RequestType.resolve(request_frame)

        if msg_type == RequestType.CREATE_QUEUE:
            response_frame = create_queue_handler(request_frame)
        elif msg_type == RequestType.PUSH_MESSAGE:
            response_frame = push_message_handler(request_frame)
        elif msg_type == RequestType.PULL_MESSAGE:
            response_frame = pull_message_handler(request_frame, self.request)
        else:
            response_frame = invalid_message_handler(request_frame)

        self.request.send(response_frame.uncompressed_bytes)

class QueueServer(ThreadingTCPServer):
    def __init__(self, server_address: tuple[str | bytes | bytearray, int], RequestHandlerClass: Callable[[Any, Any, Any], BaseRequestHandler], bind_and_activate: bool = True) -> None:
        super().__init__(server_address, RequestHandlerClass, bind_and_activate)
    
    def server_activate(self) -> None:
        logger.info('TCP Server initialized at %s', self.server_address)
        super().server_activate()

    def serve_forever(self, poll_interval: float = 0.5) -> None:
        logger.info('TCP Server started')
        super().serve_forever(poll_interval)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    socket_address = ('127.0.0.1', 9999)
    tcp_server = QueueServer(socket_address, RequestHandler)
    tcp_server.serve_forever()
