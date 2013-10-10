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
        while True:
            m = socket.recv_multipart()
            req_id, msg = m
            print 'end received', m
            x = [req_id, msg + ' back']
            time.sleep(1)
            socket.send_multipart(x)


    t = threading.Thread(target=end)
    t.daemon = True
    t.start()

    listener = Listener(context)
    listener.start()
    listener.connect('inproc://test')

    for x in xrange(10):
        f = listener.request('foo')
        print f.result(2)
