import logging
import uuid

import msgpack
import zmq

import beehive


# This is usually required to emulate a zeromq REQ socket,
# which uses an empty frame as an envelope delimiter.
empty_frame = ''

log = logging.getLogger('client')


# TODO note about choosing gevent and diverging from the future python norm of Executor and PEP 3148

class BeehiveClient(object):
    """A beehive client makes requests to and replies from services."""

    def __init__(self, context=None):
        if not context:
            context = zmq.Context()
        self.context = context
        self.socket = context.socket(zmq.DEALER)

    @property
    def identity(self):
        """Get/set the zeromq socket identity."""
        return self.socket.get(zmq.IDENTITY)

    @identity.setter
    def identity(self, value):
        self.socket.set(zmq.IDENTITY, value)

    def connect(self, endpoint):
        """Connect this client to an endpoint.

        For example, client.connect('tcp://localhost:5555')

        """
        self.socket.connect(endpoint)

    def send(self, message):
        packed_message = self.pack(message)
        # We need to prefix an empty frame in order to make
        # a zmq.DEALER socket emulate a zmq.REQ
        self.socket.send_multipart([empty_frame, packed_message])

    def make_request_id(self):
        """Return a new, unique request ID."""
        # TODO optimize
        return uuid.uuid4().bytes

    def request(self, service_name, request_body):
        """Send a request to a service."""

        # TODO is it weird that a request doesn't include a return address?
        #      that is set later by the broker, which means the client definitely
        #      requires the broker. is that bad?
        #      A socket knows its own identity right? So it _could_ be added.
        request = {
            'opcode': beehive.core.opcodes.REQUEST,
            'service': service_name,
            'id': self.make_request_id(),
            'body': request_body,
        }
        self.send(request)
        # TODO add futures
        # http://www.python.org/dev/peps/pep-3148/#future-objects
        # http://code.google.com/p/pythonfutures/
        # http://www.gevent.org/gevent.event.html

    def reply(self, destination, request_id, reply_body):
        reply = {
            'opcode': beehive.core.opcodes.REPLY,
            'destination': destination,
            'request_id': request_id,
            'body': reply_body,
        }
        self.send(reply)

    def pack(self, message):
        return msgpack.packb(message)

    def unpack(self, message):
        return msgpack.unpackb(message)

    def recv(self):
        message = self.socket.recv_multipart()[1]
        return self.unpack(message)


class BeehiveWorker(BeehiveClient):

    def __init__(self, service_name, context=None):
        super(BeehiveWorker, self).__init__(context)
        self.service_name = service_name

    def register(self):
        self.request('beehive.management.register_worker', self.service_name)

    def unregister(self):
        self.request('beehive.management.unregister_worker', '')

    def get_work(self):
        log.info('Getting work')
        return self.recv()
