import time

import zmq

context = zmq.Context()
socket = context.socket(zmq.ROUTER)
socket.bind('tcp://*:5558')

while True:
    addr, _, msg = socket.recv_multipart()
    print addr
    time.sleep(1)
    socket.send_multipart([addr, '', 'return ' + msg])
