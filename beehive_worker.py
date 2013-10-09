import beehive

import msgpack
import zmq


class BeehiveClient(object):
    def __init__(self, context, endpoint, client_ID=None):
        self.socket = context.socket(zmq.DEALER)

        if client_ID:
            self.socket.set(zmq.IDENTITY, client_ID)

        self.socket.connect(endpoint)


    def request(self, service_name, request_body):
        msg = ['', beehive.opcodes.REQUEST, service_name, request_body]
        self.socket.send_multipart(msg)

    def reply(self, destination, reply_body):
        msg = ['', beehive.opcodes.REPLY, destination, reply_body]
        self.socket.send_multipart(msg)

    def recv(self):
        r = self.socket.recv_multipart()[1]
        return msgpack.unpackb(r)


class BeehiveWorker(BeehiveClient):

    def __init__(self, context, endpoint, service_name, worker_ID=None):
        super(BeehiveWorker, self).__init__(context, endpoint, client_ID=worker_ID)

        self.service_name = service_name

        # TODO this should have failed
        #self.request('beehive.management.register_worker', '')


    def register(self):
        self.request('beehive.management.register_worker', self.service_name)

    def unregister(self):
        self.request('beehive.management.unregister_worker', '')

    def work(self, request): pass

    def get_work(self):
        return self.recv()
