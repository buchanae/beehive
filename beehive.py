import binascii
from collections import defaultdict, deque
import logging
import time

import msgpack
import zmq


log = logging.getLogger('beehive')


class Worker(object):

    def __init__(self, address, service):

        self.address = address
        self.service = service
        self._available = False

    @property
    def available(self):
        return self._available

    @available.setter
    def available(self, value):
        if value:
            self._available = True
            self.service.add_worker(self)
        else:
            self._available = False
            self.service.remove_worker(self)

    # TODO so far, these aren't actually needed
    @property
    def _key(self):
        return self.address, self.service

    def __eq__(self, other):
        return isinstance(other, Worker) and self._key == other._key

    def __hash__(self):
        return hash(self._key)


class Service(object):
    def __init__(self):
        self.request_queue = deque()
        self.worker_queue = deque()
        self.on_work = set()

    def add_worker(self, worker):
        self.worker_queue.append(worker)
        self.trigger_work()

    def remove_worker(self, worker):
        self.worker_queue.remove(worker)

    def add_request(self, reply_address, request):
        self.request_queue.append((reply_address, request))
        self.trigger_work()

    def trigger_work(self):
        while self.request_queue and self.worker_queue:
            request = self.request_queue.popleft()
            worker = self.worker_queue.popleft()

            for callback in self.on_work:
                callback(request, worker)

    @property
    def requests(self):
        return list(self.request_queue)

    # TODO change name to idle
    @property
    def waiting(self):
        return list(self.worker_queue)


class Error(Exception): pass
class ErrorTODO(Error): pass
class ReservedNameError(Error): pass
class DuplicateWorker(Error): pass
class MultipleRegistrationError(Error): pass
# TODO errors should be caught and return to client, if appropriate


class opcodes:
    # TODO optimize
    REQUEST = 'request'
    REPLY = 'reply'


class ZMQChannel(object):
    
    # TODO should I pass channel to Broker instead?
    def __init__(self, broker, context=None, poll_interval=2500):
        self.broker = broker

        if not context:
            context = zmq.Context()
        self.context = context

        self.poll_interval = poll_interval

        self.socket = self.context.socket(zmq.ROUTER)
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

        self.broker.on_send.add(self.send)

    def bind(self, endpoint):
        self.socket.bind(endpoint)

    # TODO this isn't a very nice/consistent API
    def send(self, address, message):
        # TODO serialization is hardcoded here
        #      this also breaks a couple tests
        serialized = msgpack.packb(message)
        self.socket.send_multipart([address, '', serialized])

    def start(self):

        while True:
            items = self.poller.poll(self.poll_interval)

            if items:
                message = self.socket.recv_multipart()
                self.broker.message(message)


# TODO is there a way to do this via string.format?
def address_str(address):
    try:
        return address.encode('ascii')
    except UnicodeDecodeError:
        return binascii.hexlify(address)

    
class Broker(object):

    def __init__(self, internal_prefix='beehive'):

        # TODO would be nice to have a service that you could query for information
        #      on idle services/workers
        self.internal_prefix = internal_prefix
        self._internal_services = {}
        self.internal_service('management.register_worker', self.register)
        self.internal_service('management.unregister_worker', self.unregister)
        self.internal_service('management.list_services', self.list)

        def make_service():
            # TODO dependency injection
            service = Service()
            service.on_work.add(self.service_work)
            return service

        self.services = defaultdict(make_service)
        self.workers = {}

        # TODO need to think about renaming a lot of this stuff
        # TODO this is unordered. would order ever matter?
        self.on_send = set()

    def internal_service(self, name, callback):
        name = self.internal_prefix + '.' + name
        self._internal_services[name] = callback

    def destroy(self):
        # TODO
        pass

    def send(self, address, message):
        for listener in self.on_send:
            listener(address, message)

    def message(self, message):
        sender, _, header = message[:3]
        rest = message[3:]

        assert _ == ''

        if header == opcodes.REQUEST:
            self.request(sender, rest)
        elif header == opcodes.REPLY:
            self.reply(sender, rest)
        else:
            raise InvalidCommand(header)

        # TODO necessary?
        # TODO how to do work on a regular interval with this new separation
        #      of message channel and broker?
        #self.purge_workers()


    def service_work(self, request, worker):
        log.info('Servicing request {}'.format(request))
        self.send(worker.address, request)


    def add_worker(self, worker):
        if worker.address in self.workers:
            raise DuplicateWorker()

        self.workers[worker.address] = worker
        worker.available = True


    def remove_worker(self, worker):
        # TODO catch KeyError here
        del self.workers[worker.address]
        worker.available = False
        # TODO send disconnect?


    def register(self, worker_address, service_name):

        # TODO move to add_worker?
        if service_name.startswith(self.internal_prefix):
            msg = self.internal_prefix + '.* is reserved for internal sevices'
            raise ReservedNameError(msg)

        # TODO validate message. an empty service name would pass
        service = self.services[service_name]

        # TODO what happens when you register two workers with the same identity
        #      what does zmq do about duplicate identities connecting to a router?
        worker = Worker(worker_address, service)

        try:
            self.add_worker(worker)

            log_msg = 'Registered worker {} for service {}'
            log_msg = log_msg.format(address_str(worker.address), service_name)
            log.info(log_msg)

        except DuplicateWorker:
            # TODO can't I just ignore this?
            #      I guess it's possible that the worker could send ready, then
            #      receive work and start working, and for some reason the ready
            #      message could be received again. then you'd want to be careful not
            #      to send more work to the worker since it's already working.
            #      Futhermore, this makes it possible for a misbehaving worker
            #      to affect other workers/services that are behaving fine, e.g. if
            #      a worker from service A tries to register a worker name that 
            #      service B already registered, then service A's worker should be
            #      denied, but service B's worker should remain unharmed because it was
            #      already functioning properly. right? double check that thought.
            self.remove_worker(worker)
            msg = 'Multiple "register_worker" commands were received from this worker'
            raise MultipleRegistrationError(msg)


    def unregister(self, worker_address, message):
        try:
            worker = self.workers[worker_address]
            self.remove_worker(worker)

            log_msg = 'Unregistered worker {}'
            log_msg = log_msg.format(address_str(worker.address))
            log.info(log_msg)

        except KeyError:
            # TODO should return an appropriate error?
            pass


    def request(self, client_address, message):
        service_name, request_body = message

        try:
            callback = self._internal_services[service_name]
            log.info('Processing internal service request')
            callback(client_address, request_body)

        except KeyError:
            # The request is for an external service
            service = self.services[service_name]
            log.info('Queueing request for {}'.format(service_name))
            service.add_request(client_address, request_body)

           # TODO  self.purge_workers()


    def reply(self, worker_address, message):
        client_address, reply_body = message
        # TODO this should include job/request ID so the client knows
        #      what it's getting if it was an async request
        log.info('Replying to {} with {}'.format(client_address, reply_body))
        self.send(client_address, reply_body)
        worker = self.workers[worker_address]
        worker.available = True


    def list(self, sender):
        pass


    def exists(self, sender):
        if service_name in self.broker.services:
            '200'
        else:
            '404'

# TODO possibly a service for starting workers?
