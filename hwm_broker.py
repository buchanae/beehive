import zmq


context = zmq.Context()
socket = context.socket(zmq.ROUTER)
socket.bind('tcp://*:5558')

for x in xrange(100):
    client_addr = socket.recv()
    request = socket.recv()
    print request
