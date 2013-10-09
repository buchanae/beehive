import logging

import msgpack
import zmq

import beehive


log = logging.getLogger('client')


class BeehiveClient(object):
    def __init__(self, endpoint, client_ID=None, context=None):
        if not context:
            context = zmq.Context()
        self.context = context
        self.socket = context.socket(zmq.DEALER)

        if client_ID:
            self.socket.set(zmq.IDENTITY, client_ID)

        self.socket.connect(endpoint)


    def request(self, service_name, request_body):
        msg = ['', beehive.opcodes.REQUEST, service_name, request_body]
        self.socket.send_multipart(msg)

    def reply(self, destination, reply_body):
        log.info('Replying')
        msg = ['', beehive.opcodes.REPLY, destination, reply_body]
        self.socket.send_multipart(msg)

    def recv(self):
        message = self.socket.recv_multipart()[1]
        unpacked = msgpack.unpackb(message)
        return unpacked


class BeehiveWorker(BeehiveClient):

    def __init__(self, endpoint, service_name, worker_ID=None, context=None):
        super(BeehiveWorker, self).__init__(endpoint, worker_ID, context)

        self.service_name = service_name

        # TODO this should have failed
        #self.request('beehive.management.register_worker', '')


    def register(self):
        self.request('beehive.management.register_worker', self.service_name)

    def unregister(self):
        self.request('beehive.management.unregister_worker', '')

    def get_work(self):
        log.info('Getting work')
        return self.recv()
