import logging
import threading
from Queue import Queue

import zmq


# This is usually required to emulate a zeromq REQ socket,
# which uses an empty frame as an envelope delimiter.
empty_frame = ''

log = logging.getLogger('channel')


class AsyncChannel(threading.Thread):

    def __init__(self, identity=None, context=None):
        super(AsyncChannel, self).__init__()

        self._identity = identity

        if not context:
            context = zmq.Context()
        self.context = context

        # TODO should be a daemon?
        self.daemon = True
        self._queue = Queue()

        self._recv_handlers = set()

    def run(self):
        log.info('Running channel')

        # TODO document that two clients having the same identity will
        #      produce unknown results and it's better to avoid.

        # TODO consider topic in zguide about non blocking sends
        #      and full message queues
        socket = self.context.socket(zmq.DEALER)

        if self._identity:
            socket.set(zmq.IDENTITY, self._identity)


        handlers = {
            'connect': lambda address: socket.connect(address),
            'disconnect': lambda address: socket.disconnect(address),

            # TODO this is relying pretty heavily on message being a list
            #      need to have some validation for that
            #
            # Prefix an empty frame to make the zmq.DEALER socket emulate a zmq.REQ
            'send': lambda message: socket.send_multipart([empty_frame] + message),
        }

        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)

        while True:
            while not self._queue.empty():
                op, args, kwargs = self._queue.get()
                handlers[op](*args, **kwargs)
                self._queue.task_done()

            # TODO figure out proper shutdown. see futures/thread.py

            socks = dict(poller.poll(1))

            if socks.get(socket) == zmq.POLLIN:
                m = socket.recv_multipart()
                # Discard the empty frame
                m.pop(0)

                for handler in self._recv_handlers:
                    handler(m)

    def _enqueue(self, op, *args, **kwargs):
        self._queue.put((op, args, kwargs))

    def on_recv(self, callback):
        self._recv_handlers.add(callback)

    def send(self, message):
        """Send a message"""
        self._enqueue('send', message)

    def connect(self, address):
        self._enqueue('connect', address)

    def disconnect(self, address):
        self._enqueue('disconnect', address)
