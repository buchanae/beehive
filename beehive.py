from collections import defaultdict, deque
import time

import zmq


DEFAULT_HEARTBEAT_INTERVAL = 2.5 # seconds
DEFAULT_LIVENESS = 3

DEFAULT_WORKER_EXPIRATION = 2.5 # seconds


class Worker(object):
    # TODO I don't like the name liveness

    def __init__(self, address, service, expiry_interval=DEFAULT_WORKER_EXPIRATION):

        self.address = address
        self.service = service
        # TODO bad name
        self.expiry_interval = expiry_interval
        self.heartbeat()
        self._available = False

    @property
    def expired(self):
        return time.time() > self.expiry

    def heartbeat(self):
        self.expiry = time.time() + self.expiry_interval

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

    def add_worker(self, worker):
        self.worker_queue.append(worker)

    def remove_worker(self, worker):
        self.worker_queue.remove(worker)

    def add_request(self, reply_address, request):
        self.request_queue.append((reply_address, request))

    def work(self):
        while self.request_queue and self.worker_queue:
            request = self.request_queue.popleft()
            worker = self.worker_queue.popleft()
            yield request, worker

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
class MultipleReadyCommandsError(Error): pass


class opcodes:
    # TODO optimize
    REQUEST = 'request'
    REPLY = 'reply'


class ZMQChannel(object):
    
    def __init__(self, context, broker):
        self.broker = broker

        # TODO passing in context? does that suck? i can't decide
        self.context = context
        self.socket = self.context.socket(zmq.ROUTER)
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

    def bind(self, endpoint):
        self.socket.bind(endpoint)

    def start(self):

        while True:
            items = self.poller.poll(2500)

            if items:
                message = self.socket.recv_multipart()
                self.broker.on_message(message)



class Broker(object):

    INTERNAL_SERVICE_PREFIX = 'beehive'

    def __init__(self, heartbeat_interval=DEFAULT_HEARTBEAT_INTERVAL):

        # TODO would be nice to have a service that you could query for information
        #      on idle services/workers
        self._internal_services = {}
        self.internal_service('management.register_worker', self.on_register)
        self.internal_service('management.unregister_worker', self.on_unregister)
        self.internal_service('management.worker_heartbeat', self.on_heartbeat)
        self.internal_service('management.list_services', self.on_list)

        # TODO figure out liveness and heartbeating
        self.heartbeat_interval = heartbeat_interval
        self.last_heartbeat = 0

        self.services = defaultdict(Service)
        self.workers = {}

        # TODO need to think about renaming a lot of this stuff
        # TODO this is unordered. would order ever matter?
        self.on_send = set()

    def internal_service(self, name, callback):
        name = self.INTERNAL_SERVICE_PREFIX + '.' + name
        self._internal_services[name] = callback

    def destroy(self):
        # TODO
        pass

    def send(self, address, message):
        for listener in self.on_send:
            listener(address, message)

    # TODO consider changing these names. on_foo is more of
    #      an event handler _registration_ convention than an event handler name
    def on_message(self, message):
        sender = message.pop(0)
        empty = message.pop(0)
        assert empty == ''
        header = message.pop(0)

        if header == opcodes.REQUEST:
            self.on_request(sender, message)
        elif header == opcodes.REPLY:
            self.on_reply(sender, message)
        else:
            raise InvalidCommand()

        # TODO necessary?
        # TODO how to do work on a regular interval with this new separation
        #      of message channel and broker?
        #self.purge_workers()
        #self.heartbeat()


    def add_worker(self, worker):
        if worker.address in self.workers:
            raise DuplicateWorker()

        self.workers[worker.address] = worker
        worker.available = True

        # TODO this is duplicated with on_request. need some organization here.
        # TODO consider moving this to Service.on_work that Broker subscribes to
        for request, worker in worker.service.work():
            self.send(worker.address, request)


    def remove_worker(self, worker):
        # TODO catch KeyError here
        del self.workers[worker.address]
        worker.available = False
        # TODO send disconnect?


    def on_register(self, worker_address, service_name):

        # TODO move to add_worker?
        if service_name.startswith(self.INTERNAL_SERVICE_PREFIX):
            msg = self.INTERNAL_SERVICE_PREFIX + '.* is reserved for internal sevices'
            raise ReservedNameError(msg)

        service = self.services[service_name]

        # TODO worker should tell broker its expiration time
        heartbeat_interval = 'TODO'

        # TODO what happens when you register two workers with the same identity
        #      what does zmq do about duplicate identities connecting to a router?
        worker = Worker(worker_address, service)

        try:
            self.add_worker(worker)
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


    def on_unregister(self, worker_address, message):
        try:
            worker = self.workers[worker_address]
            self.remove_worker(worker)
        except KeyError:
            pass


    def on_request(self, client_address, message):
        service_name, request_body = message

        try:
            callback = self._internal_services[service_name]
            callback(client_address, request_body)

        except KeyError:
            # The request is for an external service
            service = self.services[service_name]
            service.add_request(client_address, request_body)

           # TODO  self.purge_workers()
            
            for request, worker in service.work():
                self.send(worker.address, request)


    def on_reply(self, worker_address, message):
        client_address, reply_body = message
        # TODO this should include job/request ID so the client knows
        #      what it's getting if it was an async request
        self.send(client_address, reply_body)
        worker = self.workers[worker_address]
        worker.available = True

        # TODO this is duplicated with on_request. need some organization here.
        for request, worker in worker.service.work():
            self.send(worker.address, request)


    def on_list(self, sender):
        pass


    def on_exists(self, sender):
        if service_name in self.broker.services:
            '200'
        else:
            '404'


    def on_heartbeat(self, sender):
        # TODO worker's service name will be included in sender identity
        #      broker will provide a nice way to lookup workers via services
        try:
            worker = self.broker.workers[worker_address]
        except NoSuchWorker:
            self.send_disconnect(worker)
            raise ErrorTODO
        else:
            worker.extend_expiry()


    def purge_workers(self):
        # TODO what if you have thousands of workers? this is going to be costly.
        #      could just purge them as I go to use them, and have a periodic
        #      cleanup of old ones?
        for worker in self.workers.values():
            if worker.expired:
                self.remove_worker(worker)

    @property
    def should_heartbeat(self):
        return time.time() > self.last_heartbeat + self.heartbeat_interval

    def heartbeat(self):
        if self.should_heartbeat:
            for service in self.services.values():
                for worker in service.waiting:
                    self.send_to_worker(worker, heartbeat, None, None)

            self.last_heartbeat = time.time()
