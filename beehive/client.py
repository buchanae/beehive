from itertools import count
import logging
import threading
from Queue import Queue

from futures import Future
import msgpack
import zmq

import beehive


# This is usually required to emulate a zeromq REQ socket,
# which uses an empty frame as an envelope delimiter.
empty_frame = ''

log = logging.getLogger('client')


# TODO Listener is a poor name
class Listener(threading.Thread):
    def __init__(self, identity=None, context=None):
        super(Listener, self).__init__()

        self._identity = identity

        if not context:
            context = zmq.Context()
        self.context = context

        # TODO should be a daemon?
        self.daemon = True
        self._request_queue = Queue()
        self._management_queue = Queue()

    def run(self):
        log.info('Running listener')

        request_IDs = (str(ID) for ID in count())
        request_futures = {}
        socket = self.context.socket(zmq.DEALER)

        # TODO document that two clients having the same identity will
        #      produce unknown results and it's better to avoid.

        # TODO consider topic in zguide about non blocking sends
        #      and full message queues

        if self._identity:
            socket.set(zmq.IDENTITY, self._identity)

        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)

        handlers = {
            'connect': lambda address: socket.connect(address),
            'disconnect': lambda address: socket.disconnect(address),
        }

        while True:
            while not self._management_queue.empty():
                op, args, kwargs = self._management_queue.get()

                try:
                    handler = handlers[op]
                except KeyError:
                    pass
                else:
                    handler(*args, **kwargs)

            # TODO figure out proper shutdown. see futures/thread.py

            while not self._request_queue.empty():
                message, future = self._request_queue.get()

                # Store the future so that we can access it by request_ID
                # when the reply arrives.
                request_ID = request_IDs.next()
                request_futures[request_ID] = future

                # Prefix an empty frame to make the zmq.DEALER socket emulate a zmq.REQ
                socket.send_multipart([empty_frame, request_ID, message])

                self._request_queue.task_done()
                log.info('Sent request')

            socks = dict(poller.poll(1))

            if socks.get(socket) == zmq.POLLIN:
                log.info('Waiting for reply')
                _, request_ID, reply = socket.recv_multipart()
                future = request_futures[request_ID]
                future.set_result(reply)


    # TODO request should be send? or should have a separate send method?
    def request(self, message):
        future = Future()
        request = message, future
        self._request_queue.put(request)
        return future

    def _manage(self, op, *args, **kwargs):
        self._management_queue.put((op, args, kwargs))

    def connect(self, address):
        self._manage('connect', address)

    def disconnect(self, address):
        self._manage('disconnect', address)


class Client(object):
    """A beehive client makes requests to and replies from services."""

    def __init__(self, listener=None):
        if not listener:
            listener = Listener()
            listener.start()

        self.listener = listener

    def connect(self, address):
        """Connect this client to an endpoint.

        For example, client.connect('tcp://localhost:5555')

        """
        self.listener.connect(address)

    def pack(self, message):
        return msgpack.packb(message)

    def unpack(self, message):
        return msgpack.unpackb(message)

    def send(self, message):
        packed_message = self.pack(message)
        return self.listener.request(message)

    def request(self, service_name, request_body):
        """Send a request to a service."""

        # TODO is it weird that a request doesn't include a return address?
        #      that is set later by the broker, which means the client definitely
        #      requires the broker. is that bad?
        #      A socket knows its own identity right? So it _could_ be added.

        # TODO if the request body was very large, we'd have to unpack all that
        #      in the broker just to get the address. also, the broker needs to
        #      understand the serialization of the message. seems like the envelope
        #      should have its own frame.
        request = {
            'opcode': beehive.core.opcodes.REQUEST,
            'service': service_name,
            'body': request_body,
        }
        return self.send(request)

    def reply(self, destination, request_ID, reply_body):
        reply = {
            'opcode': beehive.core.opcodes.REPLY,
            'destination': destination,
            'body': reply_body,
        }
        return self.send(reply)


class Worker(Client):

    def __init__(self, service_name, context=None):
        super(Worker, self).__init__(context)
        self.service_name = service_name

    def register(self):
        self.request('beehive.management.register_worker', self.service_name)

    def unregister(self):
        self.request('beehive.management.unregister_worker', '')

    def get_work(self):
        log.info('Getting work')
        return self.recv()
