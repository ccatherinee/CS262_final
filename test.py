from classes import MRJob

class MRWordFreqCount(MRJob):
    def mapper(self, _, line):
        for word in line.split(' '):
            yield word.lower(), 1
    def reducer(self, k, v): 
        yield k, sum(v)
if __name__ == '__main__':
    blah = MRWordFreqCount()
    blah.run([(0, "the a the"), (1, "four a the")])

# the [1, 1, 1] cat[1, 1] a[1, 1]
# that[1]