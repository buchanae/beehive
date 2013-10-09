import logging
import threading
import time

from tl.testing import thread

from beehive.core import Broker, ZMQChannel
from beehive.client import BeehiveClient, BeehiveWorker


logging.basicConfig(level=logging.INFO)


class BeehiveSimulationTest(thread.ThreadAwareTestCase):

    endpoint = 'tcp://127.0.0.1:5555'
    
    def make_broker(self):
        channel = ZMQChannel()
        channel.bind(self.endpoint)
        broker = Broker(channel)
        channel.start()

    def make_worker(self):
        name = 'worker'
        worker = BeehiveWorker('test_service')
        worker.identity = name
        worker.connect(self.endpoint)
        worker.register()

        work = worker.get_work()
        self.assertEqual(work, ['client', 'foobar'])
        client_address, request_body = work
        worker.reply(client_address, 'reply ' + request_body)

    def make_client(self):
        name = 'client'
        client = BeehiveClient()
        client.identity = name
        client.request('test_service', 'foobar')
        reply = client.recv()
        self.assertEqual(reply, 'reply foobar')

    def test_simulation(self):
        self.run_in_thread(self.make_broker)

        time.sleep(0.1)

        self.run_in_thread(self.make_worker)
        self.run_in_thread(self.make_client)

        # TODO I'm not sure what is the best way to wait for the threads
        #      simple thread.join() doesn't work, because the channel loops
        #      forever. Possibly some kind of shutdown signal could be sent.
        #
        #      For now, the simplest solution is to sleep
        time.sleep(0.1)
