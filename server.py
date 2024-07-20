from socketserver import BaseRequestHandler, ThreadingTCPServer

from handler import create_queue_handler, push_message_handler, RequestType, invalid_message_handler
from models.dataframe import read_message


class RequestHandler(BaseRequestHandler):
    def __init__(self, request, client_address, server: ThreadingTCPServer):
        super().__init__(request, client_address, server)

    def handle(self):
        req_msg = read_message(lambda size: self.request.recv(size))
        msg_type = RequestType.resolve(req_msg)

        if msg_type == RequestType.CREATE_QUEUE:
            res_msg = create_queue_handler(req_msg)
        elif msg_type == RequestType.PUSH_MESSAGE:
            res_msg = push_message_handler(req_msg)
        else:
            res_msg = invalid_message_handler(req_msg)

        self.request.send(res_msg.writable_uncompressed)


if __name__ == '__main__':
    socket_address = ('127.0.0.1', 9999)
    tcp_server = ThreadingTCPServer(socket_address, RequestHandler)
    tcp_server.serve_forever()
