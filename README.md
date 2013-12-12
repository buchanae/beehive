This is an experiment in using ZeroMQ and writing a service-oriented, request-reply (i.e. client-worker) broker/framework/thingy.

I wrote this only as a learning experience, the code is a mess, and I don't intend it to ever by used in "the real world".

I started by exploring the Majordomo Protocol (http://rfc.zeromq.org/spec:7), tweaking it, and exploring other ideas such as asynchronous results (i.e. futures/promises).

In retrospect, I realize that I learned a TON about networking, ZeroMQ, futures/promises, and the complexities of messaging.
