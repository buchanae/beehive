import zmq

context = zmq.Context()
socket = context.socket(zmq.DEALER)

socket.connect('tcp://localhost:5558')

for x in range(100000):
    x = str(x)
    print 'sending ' + x
    print socket.send(x)
