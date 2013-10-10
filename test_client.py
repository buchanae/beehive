import logging
import threading
import time

import zmq

from beehive.client import Client, Listener

logging.basicConfig(level=logging.INFO)


if __name__ == '__main__':
    
    context = zmq.Context()

    def end():
        socket = context.socket(zmq.REP)
        socket.bind('inproc://test')

        time.sleep(3)

        while True:
            m = socket.recv_multipart()
            req_id, msg = m
            print 'end received', m
            x = [req_id, msg + ' back']
            socket.send_multipart(x)


    t = threading.Thread(target=end)
    t.daemon = True
    t.start()

    listener = Listener(context=context)
    listener.start()
    listener.connect('inproc://test')

    futures = []
    for x in xrange(10):
        f = listener.request(str(x))
        futures.append((x, f))

    for x, f in futures:
        r = f.result(10)
        print x, r
