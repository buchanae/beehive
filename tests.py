from mock import Mock, patch
from nose.tools import assert_raises, eq_, ok_
from nose.plugins.skip import SkipTest
import zmq

from beehive import *


def test_worker_equality():
    # Workers implement __eq__ which compares by (worker.address, worker.service)
    s1 = Service()
    s2 = Service()

    a = Worker('worker1', s1)
    b = Worker('worker1', s1)
    c = Worker('worker1', s2)
    d = Worker('worker2', s1)

    eq_(a, b)
    assert a != c
    assert a != d
    assert b != c
    assert c != d


def test_worker_working():
    service = Service()
    worker = Worker('address', service)

    eq_(worker.working, False)
    eq_(service.waiting, [])

    # When worker.working is set to True,
    # the worker adds itself to its service's queue
    worker.working = True
    eq_(service.waiting, [worker])

    # This looks like a funny thing to test, but since Worker.working is a property
    # I want to make sure it returns the proper value.
    eq_(worker.working, True)

    # When worker.working is set to False,
    # the worker removes itself from its service's queue
    worker.working = False
    eq_(service.waiting, [])
    eq_(worker.working, False)


@patch('time.time')
def test_worker_expiry(mock_time):
    # Mock the value returned by python's time.time() (current time)
    mock_time.return_value = 10

    # Make sure that Worker starts out with an initial expiry time
    # current time + expiry interval
    # If not expiry interval is given, use a default value
    w = Worker('one', 'foo')
    eq_(w.expiry, mock_time.return_value + DEFAULT_WORKER_EXPIRATION)

    # Same as above, but give an explicit expiry interval
    expiration_interval = 20
    w = Worker('one', 'foo', expiration_interval) 
    eq_(w.expiry, mock_time.return_value + expiration_interval)

    # Worker should be expired yet, since expiry is in the future
    assert not w.expired

    # Set the current time to a future time that is past the expiry time.
    # Worker should be expired now.
    mock_time.return_value = w.expiry + 1
    assert w.expired

    # Now "heartbeat" the worker, which updates its expiry time
    w.heartbeat()
    assert not w.expired
    eq_(w.expiry, mock_time.return_value + w.expiry_interval)


def test_service():
    s = Service()

    # The service starts out with no work to do.
    eq_(list(s.work()), [])

    # Add a worker
    s.add_worker('worker1')
    eq_(s.waiting, ['worker1'])

    # Still no work to be done
    eq_(list(s.work()), [])

    # Add a request
    s.add_request('request1')

    # Now there's work to be done
    eq_(list(s.work()), [('request1', 'worker1')])

    # Getting the work removes it, so now there's no work
    eq_(list(s.work()), [])


def test_broker_add_remove_worker():
    broker = Broker()
    s1 = Service()

    # Broker tracks workers with a dictionary.
    # Initially it's empty
    eq_(broker.workers, {})

    # Service's worker queue is empty
    eq_(s1.waiting, [])

    # Add a worker
    w = Worker('worker1', s1)
    broker.add_worker(w)

    # Broker tracks the worker
    eq_(broker.workers, {'worker1': w})

    # The broker adds the worker to the service's worker queue
    eq_(s1.waiting, [w])

    # Trying to add the same worker twice is an error
    with assert_raises(DuplicateWorker):
        broker.add_worker(w)

    broker.remove_worker(w)

    eq_(broker.workers, {})
    eq_(s1.waiting, [])

    # TODO sends worker disconnect signal?


def test_simple_connection():
    raise SkipTest()

    # TODO redo this to use BrokerChannel

    broker = Broker()
    broker_channel.bind('inproc://test_register_worker')

    # TODO optimize
    # TODO better to pass context to broker, or use context created by broker?
    #      probably better to have the option to pass in a context
    socket.connect('inproc://test_register_worker')


def test_register_unregister_worker():
    broker = Broker()
    worker_address = 'worker1'
    empty_frame = ''
    service_name = 'test_service'

    # Send a message to the broker telling it to register worker1 for test_service
    msg = [worker_address, empty_frame, opcodes.REQUEST,
           'beehive.management.register_worker', service_name]

    broker.on_message(msg)

    # Check that the broker is now tracking the worker and the service
    eq_(len(broker.workers), 1)

    eq_(broker.services.keys(), [service_name])
    eq_(broker.workers.keys(), [worker_address])

    # Get the Worker and Service objects from the broker
    worker = broker.workers[worker_address]
    service = broker.services[service_name]

    # Check that the worker is in the service's queue
    eq_(service.waiting, [worker])
    eq_(worker.service, service)
    eq_(worker.working, True)

    # Now send a message to the broker telling it to unregister the worker
    msg = [worker_address, empty_frame, opcodes.REQUEST,
           'beehive.management.unregister_worker', service_name]

    broker.on_message(msg)
    eq_(broker.workers, {})
    # The service stays registered, even though it doesn't have any workers
    eq_(broker.services.keys(), [service_name])
    eq_(service.waiting, [])


def test_register_duplicate_worker():
    raise SkipTest()

def test_unregister_unknown_worker():
    raise SkipTest()


def test_send_listener():
    # Set up a broker and add a "send" listener
    broker = Broker()
    listener = Mock()
    broker.on_send.add(listener)

    # The broker sends a message...
    broker.send('some message')

    # ...and the message is passed to the send listeners
    listener.assert_called_once_with('some message')
