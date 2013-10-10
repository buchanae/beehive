import logging
import threading
import uuid
from Queue import Queue

from futures import Future
import msgpack
import zmq

import beehive


# This is usually required to emulate a zeromq REQ socket,
# which uses an empty frame as an envelope delimiter.
empty_frame = ''

log = logging.getLogger('client')


class Listener(threading.Thread):
    def __init__(self, context=None):
        super(Listener, self).__init__()
        if not context:
            context = zmq.Context()
        self.context = context

        # TODO should be a daemon?
        self.daemon = True
        self._request_queue = Queue()
        self._management_queue = Queue()

    def run(self):
        log.info('Running listener')

        request_futures = {}
        socket = self.context.socket(zmq.DEALER)
        # TODO should each client have its own listener thread, so that each client can have its own identity?
        #      does socket identity really matter that much? client identity could be sent as part of the message.
        #      what would happen if two clients gave the same identity?
        #
        #      A related topic is mentioned in the zguide regarding full messages queues and blocking sends.
        #
        #      if request IDs aren't globally unique, then client id needs to be unique per process so that
        #      two clients wouldn't both be sent the same reply, while only one would have the valid request id.
        #socket.set(zmq.IDENTITY, identity)

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

            while not self._request_queue.empty():
                req = self._request_queue.get()

                if req is not None:
                    outgoing, future = req
                    request_id = self.make_request_id()
                    request_futures[request_id] = future
                    # We need to prefix an empty frame in order to make
                    # a zmq.DEALER socket emulate a zmq.REQ
                    msg = [empty_frame, request_id, outgoing]
                    socket.send_multipart(msg)
                    log.info('foob')
                    self._request_queue.task_done()
                    log.info('Sent request')

            socks = dict(poller.poll(1))

            if socks.get(socket) == zmq.POLLIN:
                log.info('Waiting for reply')
                _, request_id, reply = socket.recv_multipart()
                future = request_futures[request_id]
                future.set_result(reply)


            # TODO figure out proper shutdown. see futures/thread.py

    def make_request_id(self):
        """Return a new request ID.

        This must be guaranteed to be unique .... TODO.
        Doesn't this only need to be unique within the Listener?
        If so, couldn't it just be an incrementing number?
        
        """
        # TODO optimize?
        return uuid.uuid4().bytes

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

    @property
    def identity(self):
        """Get/set the zeromq socket identity."""
        # TODO
        pass

    @identity.setter
    def identity(self, value):
        # TODO
        pass

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
        return self.channel.send(message) # TODO channel/listener/whatever send/request/whatever

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
        return self.send(request)

    def reply(self, destination, request_id, reply_body):
        reply = {
            'opcode': beehive.core.opcodes.REPLY,
            'destination': destination,
            'request_id': request_id,
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
