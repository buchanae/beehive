import itertools
import time

import zmq


MAX_TRIES = 3
REQUEST_TIMEOUT = 2500


class Failed(Exception): pass

# TODO create a future
class Job(object):
    def __init__(self, context, reads_path, index_path, alignments_path):
        self.context = context
        self.message = ' '.join([reads_path, index_path, alignments_path])
        self.poller = zmq.Poller()
        self.tries_left = MAX_TRIES
        self._connect_and_send()

    def _connect_and_send(self):
        if self.tries_left == 0:
            raise Failed()

        self.tries_left -= 1
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect('tcp://localhost:5551')
        self.poller.register(self.socket, zmq.POLLIN)
        self.socket.send(self.message)

    def _disconnect(self):
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.close()
        self.poller.unregister(self.socket)

    def _retry(self):
        self._disconnect()
        self._connect_and_send()

    def retrieve(self):

        while True:
            socks = dict(self.poller.poll(REQUEST_TIMEOUT))

            if socks.get(self.socket) == zmq.POLLIN:
                reply = self.socket.recv()
                return reply

            else:
                print 'no response, retrying...'
                self._retry()


# TODO error handling. what if reads worker fails? can't find reads/index/output?
#      need zeroRPC's exception handling


# TODO if broker crashes and comes back, this needs to be able to retry the messages
context = zmq.Context()

jobs = []

for chunk_i in range(10):
    chunk_path = 'reads_chunk_{}.fas'.format(chunk_i)
    alignments_path = 'output_{}.sam'.format(chunk_i)

    job = Job(context, chunk_path, 'bt_index', alignments_path)
    jobs.append(job)

print 'sent jobs'

for job in jobs:
    print job.retrieve()
