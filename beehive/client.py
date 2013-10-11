from itertools import count
import logging

from futures import Future
import msgpack

import beehive
from beehive.core import opcodes
from beehive.channel import AsyncChannel


log = logging.getLogger('client')


class Client(object):

    # TODO find a nice way to make channel optional
    def __init__(self, channel):
        self._request_IDs = (str(ID) for ID in count())
        self._request_futures = {}
        self._request_handlers = set()

        self.channel = channel
        self.channel.on_recv(self.handle_recv)

    def on_request(self, handler):
        self._request_handlers.add(handler)

    def pack(self, message):
        return msgpack.packb(message)

    def unpack(self, message):
        return msgpack.unpackb(message)

    def handle_recv(self, message):
        opcode = message.pop(0)

        if opcode == opcodes.REPLY:
            request_ID = message.pop(0)
            try:
                future = self._request_futures[request_ID]
                future.set_result(message)
                # TODO make use of set_exception
            except KeyError:
                # TODO
                pass

        elif opcode == opcodes.REQUEST:
            for handler in self._request_handlers:
                handler(message)

        else:
            # TODO invalid opcode is a better name?
            raise InvalidCommand(opcode)

    def _send(self, opcode, address, request_ID, body):
        packed_body = self.pack(body)
        message = [opcode, address, request_ID, body]
        self.channel.send(message)

    def send_request(self, service, body):
        future = Future()

        # Store the future by request_ID
        # so that we can retrieve it when the reply arrives.
        request_ID = self._request_IDs.next()
        self._request_futures[request_ID] = future
        self._send(opcodes.REQUEST, service, request_ID, body)
        return future

    def send_reply(self, destination, request_ID, body):
        self._send(opcodes.REPLY, destination, request_ID, body)


class Worker(Client):

    def __init__(self, service_name, context=None):
        super(Worker, self).__init__(context)
        self.service_name = service_name
        self.on_request(self.handle_request)

    def register(self):
        self.request('beehive.management.register_worker', self.service_name)

    def unregister(self):
        self.request('beehive.management.unregister_worker', '')

    def handle_request(self, message):
        log.info('Got work')
