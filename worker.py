from __future__ import print_function

import subprocess
import sys
import time

import zmq


context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect('tcp://localhost:5552')

# TODO handle all bowtie options
#      especially: input file format, paired-end

# TODO write to file/db? or act as database with queryable interface? or both?
#      as a DB means much more traffic over the network when streaming large
#      result sets.

# TODO file chunk reader


socket.send('READY')

while True:
    address, _, request = socket.recv_multipart()

    reads, index, output = request.split()

    cmd = 'bowtie2 -f -x {index} -U {reads} -S {output}'

    conf = {
        'index': index,
        'reads': reads,
        'output': output,
    }
    cmd = cmd.format(**conf)

    subprocess.check_call(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          shell=True)
    time.sleep(2)
    print('.', file=sys.stderr, end='')

    socket.send_multipart([address, '', 'done'])
