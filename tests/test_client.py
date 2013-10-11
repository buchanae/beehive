import logging
import threading
import time

import zmq

from beehive.client import Client
from beehive.channel import AsyncChannel


logging.basicConfig(level=logging.INFO)


if __name__ == '__main__':
    
    context = zmq.Context()

    def bouncer():
        socket = context.socket(zmq.REP)
        socket.bind('inproc://test')

        time.sleep(3)

        while True:
            m = socket.recv_multipart()
            logging.info('end received {}'.format(m))
            opcode = m.pop(0)
            addr = m.pop(0)
            x = ['reply'] + m + ['return']
            socket.send_multipart(x)


    t = threading.Thread(target=bouncer)
    t.daemon = True
    t.start()

    channel = AsyncChannel(context=context)
    channel.connect('inproc://test')
    channel.start()

    time.sleep(0.5)

    client = Client(channel=channel)

    def handle_recv(message):
        print message

    #channel.on_recv(handle_recv)

    futures = []
    for x in (str(x) for x in xrange(10)):
        #channel.send([str(x)])
        # TODO woah, got a segfault from python. weird.
        #      something to do with threading
        #
        # f = client.request()
        # TypeError: request() takes exactly 3 arguments (1 given)
        # Segmentation fault

        f = client.send_request('service name', x)
        futures.append((x, f))

    #time.sleep(10)
    for x, f in futures:
        r = f.result(10)
        print x, r
