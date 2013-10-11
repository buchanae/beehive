import logging
import threading
import time

import zmq

from beehive.client import Client, Listener

logging.basicConfig(level=logging.INFO)


if __name__ == '__main__':
    
    context = zmq.Context()

    def bouncer():
        socket = context.socket(zmq.REP)
        socket.bind('inproc://test')

        time.sleep(3)

        while True:
            m = socket.recv_multipart()
            req_id = m.pop(0)
            print 'end received', m
            x = [req_id, 'return'] + m
            socket.send_multipart(x)


    t = threading.Thread(target=bouncer)
    t.daemon = True
    t.start()

    listener = Listener('inproc://test', context=context)
    listener.start()

    time.sleep(0.5)

    client = Client(listener)


    futures = []
    for x in (str(x) for x in xrange(10)):
        #f = listener.request(str(x))
        # TODO woah, got a segfault from python. weird.
        #      something to do with threading
        #
        # f = client.request()
        # TypeError: request() takes exactly 3 arguments (1 given)
        # Segmentation fault
        f = client.request('service name', x)
        futures.append((x, f))

    for x, f in futures:
        r = f.result(10)
        print x, r
