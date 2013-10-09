from nose.tools import assert_raises, eq_, ok_

from beehive.client import *


def test_client_identity():
    client = BeehiveClient()
    eq_(client.identity, '')

    client.identity = 'foo'
    eq_(client.identity, 'foo')
