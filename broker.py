import zmq


context = zmq.Context()

frontend = context.socket(zmq.ROUTER)
backend = context.socket(zmq.DEALER)

frontend.bind('tcp://*:5551')
backend.bind('tcp://*:5552')

poller = zmq.Poller()
poller.register(frontend, zmq.POLLIN)
poller.register(backend, zmq.POLLIN)

zmq.device(zmq.QUEUE, frontend, backend)

# TODO
# This is roundrobin, so it doesn't account for how busy the worker is
# If a worker is going to take an hour, no jobs should be queued to that

frontend.close()
backend.close()
context.term()
