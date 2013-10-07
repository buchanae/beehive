import itertools
import logging
import time

from mdcliapi2 import MajorDomoClient


MAX_TRIES = 3
REQUEST_TIMEOUT = 2500

logging.basicConfig(format="%(asctime)s %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                    level=logging.INFO)


# TODO error handling. what if reads worker fails? can't find reads/index/output?
#      need zeroRPC's exception handling

client = MajorDomoClient('tcp://localhost:5555')

# TODO if broker crashes and comes back, this needs to be able to retry the messages

jobs = []

for chunk_i in range(5000):
    chunk_path = 'reads_chunk_{}.fas'.format(chunk_i)
    alignments_path = 'output_{}.sam'.format(chunk_i)

    client.send('bowtie', [chunk_path, 'bt_index', alignments_path])

print 'sent jobs'

while True:
    print client.recv()
