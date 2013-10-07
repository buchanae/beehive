import time

import zmq

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.setsockopt(zmq.IDENTITY, 'client_address')
socket.connect('tcp://localhost:5558')

while True:
    socket.send('foo')
    print socket.recv()
    time.sleep(1)
