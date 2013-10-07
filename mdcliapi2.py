"""Majordomo Protocol Client API, Python version.

Implements the MDP/Worker spec at http:#rfc.zeromq.org/spec:7.

Author: Min RK <benjaminrk@gmail.com>
Based on Java example by Arkadiusz Orzechowski
"""

import logging

import zmq

import MDP
from zhelpers import dump


class MajorDomoClient(object):
    """
    Majordomo Protocol Client API, Python version.

    Implements the MDP/Worker spec at http:#rfc.zeromq.org/spec:7.
    """

    def __init__(self, broker_address, timeout=2500):
        self.broker_address = broker_address
        self.timeout = timeout
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.client = None
        self.reconnect_to_broker()

    def reconnect_to_broker(self):
        """Connect or reconnect to broker"""

        if self.client:
            self.poller.unregister(self.client)
            self.client.close()

        logging.info("I: connecting to broker at %s...", self.broker_address)

        self.client = self.context.socket(zmq.DEALER)
        self.client.linger = 0
        self.client.connect(self.broker_address)
        self.poller.register(self.client, zmq.POLLIN)

    def send(self, service, request):
        """Send request to broker"""

        if not isinstance(request, list):
            request = [request]

        # Prefix request with protocol frames
        # Frame 0: empty (REQ emulation)
        # Frame 1: "MDPCxy" (six bytes, MDP/Client x.y)
        # Frame 2: Service name (printable string)
        request = ['', MDP.C_CLIENT, service] + request

        logging.warn("I: send request to '%s' service: ", service)
        # TODO dump should go through logging
        dump(request)

        self.client.send_multipart(request)

    def recv(self):
        """Returns the reply message or None if there was no reply."""
        items = self.poller.poll(self.timeout)

        if items:
            # if we got a reply, process it
            msg = self.client.recv_multipart()

            logging.info("I: received reply:")
            # TODO dump should go through logging
            dump(msg)

            # TODO a message validation function would be nice and reusable

            # Don't try to handle errors, just assert noisily
            assert len(msg) >= 4

            empty = msg.pop(0)
            header = msg.pop(0)
            assert MDP.C_CLIENT == header

            service = msg.pop(0)
            return msg
        else:
            # TODO
            logging.warn("W: permanent error, abandoning request")
