import logging

import zmq
from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream


# TODO service names. need a queue for each service
#      - service registry/discovery

# TODO Titanic

class LRUQueue(object):
    """LRUQueue class using ZMQStream/IOLoop for event dispatching"""

    def __init__(self, backend_socket, frontend_socket):
        self.workers = []
        
        self.backend = ZMQStream(backend_socket)
        self.frontend = ZMQStream(frontend_socket)
        self.backend.on_recv(self.handle_backend)

        self.loop = IOLoop.instance()

    def handle_backend(self, msg):
        # Queue worker address for LRU routing
        worker_addr, empty, client_addr = msg[:3]

        # add worker back to the list of workers
        self.workers.append(worker_addr)

        # on first recv, start accepting frontend messages
        self.frontend.on_recv(self.handle_frontend)

        #   Second frame is empty
        assert empty == ""

        # Third frame is READY or else a client reply address
        # If client reply, send rest back to frontend
        if client_addr != "READY":
            empty, reply = msg[3:]

            # Following frame is empty
            assert empty == ""

            self.frontend.send_multipart([client_addr, '', reply])

    def handle_frontend(self, msg):
        # Now get next client request, route to LRU worker
        # Client request is [address][empty][request]
        client_addr, empty, request = msg

        assert empty == ""

        #  Dequeue and drop the next worker address
        worker_id = self.workers.pop()
        self.backend.send_multipart([worker_id, '', client_addr, '', request])

        if not self.workers:
            # stop receiving until workers become available again
            self.frontend.stop_on_recv()


def main():

    url_frontend = 'tcp://*:5551'
    url_backend = 'tcp://*:5552'

    # Prepare our context and sockets
    context = zmq.Context()

    frontend = context.socket(zmq.ROUTER)
    backend = context.socket(zmq.ROUTER)

    frontend.bind(url_frontend)
    backend.bind(url_backend)

    # create queue with the sockets
    queue = LRUQueue(backend, frontend)

    # start reactor
    IOLoop.instance().start()


if __name__ == "__main__":
    main()
