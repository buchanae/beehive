import logging
import threading
import time

import zmq
from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream

from beehive import Broker
from beehive_worker import BeehiveClient, BeehiveWorker


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    context = zmq.Context()
    stream = ZMQStream(socket)

    broker = Broker(stream)

    time.sleep(0.1)

    def make_worker(n):
        name = 'worker {}'.format(n)
        worker = BeehiveWorker(context, 'tcp://localhost:5555', 'test_service', name)
        worker.register()
        r = worker.get_work()
        print 'worker.get_work unpacked', r
        client_address, request_body = r
        worker.reply(client_address, 'reply ' + request_body)

    t = threading.Thread(target=make_worker, args=(1,))
    t.daemon = True
    t.start()

    def make_client(n):
        name = 'client {}'.format(n)
        client = BeehiveClient(context, 'tcp://localhost:5555', name)
        client.request('test_service', 'foobar')
        r = client.recv()
        print 'client recv', r

    c = threading.Thread(target=make_client, args=(1,))
    c.daemon = True
    c.start()

    channel.start()
