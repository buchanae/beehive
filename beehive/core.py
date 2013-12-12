import binascii
from collections import defaultdict, deque
import logging
import time

import msgpack
import zmq

from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream


log = logging.getLogger('beehive')


class Error(Exception): pass
class ErrorTODO(Error): pass
class ReservedNameError(Error): pass
class DuplicateWorker(Error): pass
class MultipleRegistrationError(Error): pass
class InvalidServiceName(Error): pass
class UnknownWorker(Error): pass
class InvalidCommand(Error): pass
# TODO errors should be caught and return to client, if appropriate


class opcodes:
    # TODO optimize
    REQUEST = 'request'
    REPLY = 'reply'


# TODO caching requests in redis/memcache could add resilency
# TODO how long to hold on to results? What if the client never picks them up?
#      at some point, rabbitmq is better. build an interface where the backend
#      could be easily replaced.

class ZMQChannel(object):
    
    def __init__(self, context=None):

        self.context = context if context else zmq.Context()
        # TODO can I/should I separate the socket from the channel
        #      so that I can reuse this for clients?
        self.socket = self.context.socket(zmq.ROUTER)
        self.stream = ZMQStream(self.socket)
        self.loop = IOLoop.instance()

    def bind(self, endpoint):
        self.socket.bind(endpoint)

    def connect(self, endpoint):
        self.socket.connect(endpoint)

    def start(self):
        self.loop.start()

    def on_recv(self, *args, **kwargs):
        self.stream.on_recv(*args, **kwargs)

    def pack(self, message):
        return msgpack.packb(message)

    def send(self, address, message):
        packed = self.pack(message)
        self.stream.send_multipart([address, '', packed])


# TODO is there a way to do this via string.format?
def address_str(address):
    try:
        return address.encode('ascii')
    except UnicodeDecodeError:
        return binascii.hexlify(address)


class Worker(object):
    """The broker uses this class internally to represent a worker."""

    def __init__(self, address, service):
        """Worker constructor
        :param address: The worker's address.
        :param service: The Service instance this worker is registered to.

        """
        self.address = address
        self.service = service
        self._idle = False

    # TODO rename to idle
    @property
    def idle(self):
        """Returns True if this worker is currently idle.

        When set, this worker will be added to/removed from the service's
        idle workers accordingly.
        """
        return self._idle

    @idle.setter
    def idle(self, value):
        # TODO there _must_ be bugs here. You can add this worker to the queue twice.
        #      try,
        #      if value and not self._idle:
        #      and vice versa
        if value:
            self._idle = True
            self.service.add_worker(self)
        else:
            self._idle = False
            self.service.remove_worker(self)


class Service(object):
    """The broker uses this class internally to represent a service."""

    def __init__(self):
        self.request_queue = deque()
        self.worker_queue = deque()
        self._on_work_callbacks = set()

    @property
    def requests(self):
        """Return a list of queued requests."""
        return list(self.request_queue)

    @property
    def idle_workers(self):
        """Return a list of idle workers."""
        return list(self.worker_queue)

    def add_worker(self, worker):
        """Add a worker to the service.

        Adding a worker causes the service to process any queued requests.

        """
        self.worker_queue.append(worker)
        self.trigger_work()

    def remove_worker(self, worker):
        """Remove a worker from the service."""

        self.worker_queue.remove(worker)

    def add_request(self, reply_address, request):
        """Add a request to the queue.
        
        Adding a request causes the service to process any queued requets.

        """
        self.request_queue.append((reply_address, request))
        self.trigger_work()

    def on_work(self, callback):
        """Register a callback that will be called with a request is processed."""
        self._on_work_callbacks.add(callback)

    # TODO rename to process_work?
    def trigger_work(self):
        """Process any queued requests, if idle workers are available.
        
        When a request is processed, the "on_work" callbacks are called.
        """

        while self.request_queue and self.worker_queue:
            request = self.request_queue.popleft()
            worker = self.worker_queue.popleft()

            for callback in self._on_work_callbacks:
                callback(request, worker)

    
class Broker(object):
    """A message broker

    :param stream: A stream (TODO should be channel) of awesomeness TODO.
    :param internal_prefix: Prefix for internal services.
    
    """

    Service = Service
    Worker = Worker

    # TODO should rename stream to channel?
    # TODO service for starting workers
    def __init__(self, stream, internal_prefix='beehive'):
        self.stream = stream
        self.stream.on_recv(self.message)

        # TODO would be nice to have a service that you could query for information
        #      on idle services/workers
        self.internal_prefix = internal_prefix
        self._internal_services = {}
        self.internal_service('management.register_worker', self.register)
        self.internal_service('management.unregister_worker', self.unregister)
        self.internal_service('management.list_services', self.list)

        def make_service():
            service = self.Service()
            service.on_work(self.service_work)
            return service

        self.services = defaultdict(make_service)
        self.workers = {}

        log.debug('Broker initialized')


    def internal_service(self, name, callback):
        """Register an internal service.

        :param name: name of the internal service (without the prefix)
                     e.g. management.awesomeness

        """
        name = self.internal_prefix + '.' + name
        self._internal_services[name] = callback

    def destroy(self):
        # TODO
        pass

    def send(self, address, message):
        """Send a message from the broker to a destination.

        :param address: The destination's address.
        :param message: The message to send.
        
        """
        self.stream.send(address, message)

    def message(self, message):
        """Process an incoming message."""

        log.debug('Received message')

        sender, _, header = message[:3]
        rest = message[3:]

        assert _ == ''

        if header == opcodes.REQUEST:
            self.handle_request(sender, rest)
        elif header == opcodes.REPLY:
            self.handle_reply(sender, rest)
        else:
            # TODO invalid opcode is a better name?
            raise InvalidCommand(header)


    def service_work(self, request, worker):
        """Send a work request to a worker.

        This is a callback of Service's on_work event

        """
        log.info('Servicing request {}'.format(request))
        # TODO add opcode so that client knows this is a work request
        self.send(worker.address, request)


    def add_worker(self, worker):
        """Add a worker."""

        if worker.address in self.workers:
            raise DuplicateWorker()

        self.workers[worker.address] = worker
        worker.idle = True


    def remove_worker(self, worker):
        """Remove a worker."""

        try:
            del self.workers[worker.address]
            worker.idle = False

        except KeyError:
            raise UnknownWorker(worker.address)
        # TODO send disconnect?


    def register(self, worker_address, service_name):
        """Handle a worker registration request."""

        if service_name.startswith(self.internal_prefix):
            msg = self.internal_prefix + '.* is reserved for internal sevices'
            log.error(msg)
            raise ReservedNameError(msg)

        if not service_name:
            log.error('Invalid service name: {}'.format(service_name))
            raise InvalidServiceName(service_name)

        service = self.services[service_name]

        worker = self.Worker(worker_address, service)

        self.add_worker(worker)

        log_msg = 'Registered worker {} for service {}'
        log_msg = log_msg.format(address_str(worker.address), service_name)
        log.info(log_msg)


    def unregister(self, worker_address, message):
        """Handle a worker unregistration request."""

        try:
            worker = self.workers[worker_address]
        except KeyError:
            raise UnknownWorker(worker_address)
        else:
            self.remove_worker(worker)

            log_msg = 'Unregistered worker {}'
            log_msg = log_msg.format(address_str(worker.address))
            log.info(log_msg)

    # TODO what happens when a request is sent to a worker and that worker dies?
    #      should the client retry the request? or should the broker?

    # TODO rename to handle_request or process_request
    def handle_request(self, client_address, message):
        """Handle an incoming request."""

        # TODO how is test_simulation working? how is it getting the service name?
        service_name = message.pop(0)

        if service_name in self._internal_services:
            callback = self._internal_services[service_name]
            log.info('Processing internal service request')
            callback(client_address, message)
        else:
            # The request is for an external service
            service = self.services[service_name]
            log.info('Queueing request for {}'.format(service_name))
            service.add_request(client_address, message)


    def handle_reply(self, worker_address, message):
        """Handle a reply message from a worker to a client."""

        client_address = message.pop(0)

        log.info('Replying to {}'.format(client_address))
        self.send(client_address, message)
        worker = self.workers[worker_address]
        worker.idle = True


    def list(self, sender):
        """Handle a request to list available services."""
        pass


    def exists(self, sender):
        """Handle a request to check whether a service exists."""
        if service_name in self.broker.services:
            '200'
        else:
            '404'

# TODO possibly a service for starting workers?
