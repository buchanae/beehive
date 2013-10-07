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
        self._working = False

    @property
    def expired(self):
        return time.time() > self.expiry

    def heartbeat(self):
        self.expiry = time.time() + self.expiry_interval

    @property
    def working(self):
        return self._working

    @working.setter
    def working(self, value):
        if value:
            self._working = True
            self.service.add_worker(self)
        else:
            self._working = False
            self.service.remove_worker(self)

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

    def add_request(self, request):
        self.request_queue.append(request)

    def remove_request(self, request):
        self.request_queue.remove(request)

    def work(self):
        while self.request_queue and self.worker_queue:
            request = self.request_queue.popleft()
            worker = self.worker_queue.popleft()
            yield request, worker

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


class Broker(object):

    INTERNAL_SERVICE_PREFIX = 'beehive'

    def __init__(self, heartbeat_interval=DEFAULT_HEARTBEAT_INTERVAL):

        # TODO Move connection specifics outside broker?
        #      Does the broker have to be zmq specific?
        #      or is there a way to separate that?
        # TODO this can't use the same context. makes testing difficult
        # TODO better to pass in context, or use broker's context?
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.ROUTER)
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

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

    def internal_service(self, name, callback):
        name = self.INTERNAL_SERVICE_PREFIX + '.' + name
        self._internal_services[name] = callback

    def bind(self, endpoint):
        self.socket.bind(endpoint)

    def destroy(self):
        # TODO
        pass


    def start(self):

        while True:
            items = self.poller.poll(self.heartbeat_interval)

            print 'foo'
            if items:
                message = self.socket.recv_multipart()

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
            self.purge_workers()
            self.heartbeat()


    def add_worker(self, worker):
        if worker.address in self.workers:
            raise DuplicateWorker()

        self.workers[worker.address] = worker
        worker.working = True


    def remove_worker(self, worker):
        # TODO catch KeyError here
        del self.workers[worker.address]
        worker.working = False
        # TODO send disconnect?


    def on_request(self, sender, message):
        service_name = message.pop(0)

        try:
            callback = self.internal_services[service_name]
            callback(sender, message)

        except KeyError:
            # The request is for an external service
            # TODO message?
            service = self.services[service_name]
            service.add_request(message)

            self.purge_workers()
            
            for worker, request in service.work():
                self.send_to_worker(worker, request)


    def on_register(self, sender, service_name):

        if service_name.startswith(self.INTERNAL_SERVICE_PREFIX):
            msg = self.INTERNAL_SERVICE_PREFIX + '.* is reserved for internal sevices'
            raise ReservedNameError(msg)

        service = self.services[service_name]

        # TODO worker should tell broker its expiration time
        heartbeat_interval = 'TODO'

        # TODO what happens when you register two workers with the same identity
        #      what does zmq do about duplicate identities connecting to a router?
        worker = Worker(sender, service)

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


    def on_unregister(self, sender):
        worker_address = 'TODO'

        worker = self.workers.get(worker_address)
        if worker:
            self.remove_worker(worker)


    def on_reply(self, sender):
        # Remove & save client return envelope and insert the
        # protocol header and service name, then rewrap envelope.
        client = msg.pop(0)
        empty = msg.pop(0) # ?
        # TODO this should include job/request ID so the client knows
        #      what it's getting if it was an async request
        msg = [client, '', MDP.C_CLIENT] + msg
        self.socket.send_multipart(msg)
        self.worker_waiting(worker)


    def on_list(self, sender):
        pass


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


    def on_exists(self, sender):
        if service_name in self.broker.services:
            '200'
        else:
            '404'


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
