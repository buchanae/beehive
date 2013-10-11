from itertools import count
import logging
import threading
from Queue import Queue

from futures import Future
import msgpack
import zmq

import beehive
from beehive.core import opcodes


# This is usually required to emulate a zeromq REQ socket,
# which uses an empty frame as an envelope delimiter.
empty_frame = ''

log = logging.getLogger('client')


# TODO Listener is a poor name
class Listener(threading.Thread):
    def __init__(self, address=None, identity=None, context=None):
        super(Listener, self).__init__()


        self._identity = identity

        if not context:
            context = zmq.Context()
        self.context = context

        # TODO should be a daemon?
        self.daemon = True
        self._queue = Queue()

        if address:
            self.connect(address)

    def run(self):
        log.info('Running listener')

        # TODO document that two clients having the same identity will
        #      produce unknown results and it's better to avoid.

        # TODO consider topic in zguide about non blocking sends
        #      and full message queues
        socket = self.context.socket(zmq.DEALER)

        if self._identity:
            socket.set(zmq.IDENTITY, self._identity)


        request_IDs = (str(ID) for ID in count())
        request_futures = {}

        def handle_request(message, future):
            # Store the future by request_ID
            # so that we can retrieve it when the reply arrives.
            request_ID = request_IDs.next()
            request_futures[request_ID] = future

            # Prefix an empty frame to make the zmq.DEALER socket emulate a zmq.REQ
            socket.send_multipart([empty_frame, request_ID] + message)

            log.info('Sent request')


        def handle_send(message):
            # TODO this is relying pretty heavily on message being a list
            #      need to have some validation for that
            socket.send_multipart([empty_frame] + message)


        handlers = {
            'connect': lambda address: socket.connect(address),
            'disconnect': lambda address: socket.disconnect(address),
            'request': handle_request,
            'send': handle_send,
        }

        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)

        while True:
            while not self._queue.empty():
                op, args, kwargs = self._queue.get()

                try:
                    handler = handlers[op]
                except KeyError:
                    # TODO error here?
                    pass
                else:
                    handler(*args, **kwargs)
                    self._queue.task_done()

            # TODO figure out proper shutdown. see futures/thread.py

            socks = dict(poller.poll(1))

            if socks.get(socket) == zmq.POLLIN:
                log.info('Waiting for reply')
                m = socket.recv_multipart()
                # Discard the empty frame
                m.pop(0)
                request_ID = m.pop(0)
                future = request_futures[request_ID]
                future.set_result(m)
                # TODO make use of set_exception

    def _enqueue(self, op, *args, **kwargs):
        self._queue.put((op, args, kwargs))

    def request(self, message):
        future = Future()
        self._enqueue('request', message, future)
        return future

    def send(self, message):
        """Send a message.

        This differs from request() because I don't want a Future,
        i.e. I'm not expecting a result so I just want to send the message.
        """
        self._enqueue.put('send', message)

    def connect(self, address):
        self._enqueue('connect', address)

    def disconnect(self, address):
        self._enqueue('disconnect', address)


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

    def request(self, service_name, request_body):
        """Send a request to a service."""

        # TODO if the request body was very large, we'd have to unpack all that
        #      in the broker just to get the address. also, the broker needs to
        #      understand the serialization of the message. seems like the envelope
        #      should have its own frame.
        packed_body = self.pack(request_body)
        message = [opcodes.REQUEST, service_name, packed_body]
        return self.listener.request(message)

    def reply(self, destination, request_ID, reply_body):
        packed_body = self.pack(reply_body)
        message = [opcodes.REPLY, request_ID, packed_body]
        return self.listener.send(message)

# TODO there's a big missing piece, and a difference between worker and client
#      the client can easily rely on futures to get results, but the worker
#      needs a way to block until it has work. It needs a recv method.
#      The Listener class is very specific to futures, so how does this fit in?  
#      Do I need two different channels? One for clients and one for workers?
#      Except worker's can send requests too, and recieve replies. I worker
#      could have an internal client for that, and a separate channel for 
#      receiving work?
#
#      On a somewhat related note, it'd probably be nice to have synchronous methods
#      available, in case you don't need futures.

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
