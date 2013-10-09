import logging
import threading
import time

from beehive import Broker, ZMQChannel
from beehive_worker import BeehiveClient, BeehiveWorker


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)

    endpoint = 'tcp://127.0.0.1:5555'

    def make_broker():
        channel = ZMQChannel()
        channel.bind(endpoint)
        broker = Broker(channel)
        channel.start()

    def make_worker(n):
        name = 'worker {}'.format(n)
        worker = BeehiveWorker(endpoint, 'test_service', name)
        worker.register()

        r = worker.get_work()
        print 'worker.get_work unpacked', r
        client_address, request_body = r
        worker.reply(client_address, 'reply ' + request_body)

    def make_client(n):
        name = 'client {}'.format(n)
        client = BeehiveClient(endpoint, name)
        client.request('test_service', 'foobar')
        r = client.recv()
        print 'client recv', r


    a = threading.Thread(target=make_broker)
    a.daemon = True
    a.start()

    time.sleep(0.1)

    b = threading.Thread(target=make_worker, args=(1,))
    b.daemon = True
    b.start()

    c = threading.Thread(target=make_client, args=(1,))
    c.daemon = True
    c.start()

    time.sleep(1)
