import random

alphabet = 'ATCG'

seq = ''.join(random.choice(alphabet) for i in xrange(10000))

with open('genome.fas', 'w') as fh:
    fh.write('>reference1\n')
    fh.write(seq + '\n')

for chunk_i in xrange(10):
    path = 'reads_chunk_{}.fas'.format(chunk_i)

    with open(path, 'w') as fh:
        for i in xrange(100):
            fh.write('>read' + str(i) + '\n')
            start = random.randrange(len(seq) - 51)
            fh.write(seq[start:start + 50] + '\n')
