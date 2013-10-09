import logging

import msgpack
import zmq

import beehive


log = logging.getLogger('client')


class BeehiveClient(object):

    def __init__(self, context=None):
        if not context:
            context = zmq.Context()
        self.context = context
        self.socket = context.socket(zmq.DEALER)

    @property
    def identity(self):
        return self.socket.get(zmq.IDENTITY)

    @identity.setter
    def identity(self, value):
        self.socket.set(zmq.IDENTITY, value)

    def connect(self, endpoint):
        self.socket.connect(endpoint)

    def request(self, service_name, request_body):
        # TODO add job ID and futures
        msg = ['', beehive.opcodes.REQUEST, service_name, request_body]
        self.socket.send_multipart(msg)

    def reply(self, destination, reply_body):
        log.info('Replying')
        msg = ['', beehive.opcodes.REPLY, destination, reply_body]
        self.socket.send_multipart(msg)

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
