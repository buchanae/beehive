import zmq


if __name__ == "__main__":
    url_frontend = 'tcp://*:5551'

    context = zmq.Context()
    frontend = context.socket(zmq.ROUTER)
    frontend.bind(url_frontend)

    while True:
        client_addr = frontend.recv()
        _ = frontend.recv()
        request = frontend.recv()
        print request
