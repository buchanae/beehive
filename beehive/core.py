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
# TODO errors should be caught and return to client, if appropriate


class opcodes:
    # TODO optimize
    REQUEST = 'request'
    REPLY = 'reply'


class ZMQChannel(object):
    
    def __init__(self, context=None):

        self.context = context if context else zmq.Context()
        self.socket = self.context.socket(zmq.ROUTER)
        self.stream = ZMQStream(self.socket)
        self.loop = IOLoop.instance()

    def bind(self, endpoint):
        self.socket.bind(endpoint)

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
        self._available = False

    # TODO rename to idle
    @property
    def available(self):
        """Returns True if this worker is currently idle.

        When set, this worker will be added to/removed from the service's
        idle workers accordingly.
        """
        return self._available

    @available.setter
    def available(self, value):
        # TODO there _must_ be bugs here. You can add this worker to the queue twice.
        #      try,
        #      if value and not self._available:
        #      and vice versa
        if value:
            self._available = True
            self.service.add_worker(self)
        else:
            self._available = False
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

        sender, _, header = message[:3]
        rest = message[3:]

        assert _ == ''

        if header == opcodes.REQUEST:
            self.request(sender, rest)
        elif header == opcodes.REPLY:
            self.reply(sender, rest)
        else:
            raise InvalidCommand(header)


    def service_work(self, request, worker):
        """Send a work request to a worker.

        This is a callback of Service's on_work event

        """
        log.info('Servicing request {}'.format(request))
        self.send(worker.address, request)


    def add_worker(self, worker):
        """Add a worker."""

        if worker.address in self.workers:
            raise DuplicateWorker()

        self.workers[worker.address] = worker
        worker.available = True


    def remove_worker(self, worker):
        """Remove a worker."""

        try:
            del self.workers[worker.address]
            worker.available = False

        except KeyError:
            raise UnknownWorker(worker.address)
        # TODO send disconnect?


    def register(self, worker_address, service_name):
        """Handle a worker registration request."""

        if service_name.startswith(self.internal_prefix):
            msg = self.internal_prefix + '.* is reserved for internal sevices'
            raise ReservedNameError(msg)

        if not service_name:
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


    # TODO rename to handle_request or process_request
    def request(self, client_address, message):
        """Handle an incoming request."""

        service_name, request_body = message

        if service_name in self._internal_services:
            callback = self._internal_services[service_name]
            log.info('Processing internal service request')
            callback(client_address, request_body)
        else:
            # The request is for an external service
            service = self.services[service_name]
            log.info('Queueing request for {}'.format(service_name))
            service.add_request(client_address, request_body)


    def reply(self, worker_address, message):
        """Handle a reply message from a worker to a client."""

        client_address, reply_body = message
        # TODO this should include job/request ID so the client knows
        #      what it's getting if it was an async request
        log.info('Replying to {} with {}'.format(client_address, reply_body))
        self.send(client_address, reply_body)
        worker = self.workers[worker_address]
        worker.available = True


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
