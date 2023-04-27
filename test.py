from classes import MRJob

class MRWordFreqCount(MRJob):
    def mapper(self, _, line):
        for word in line.split(' '):
            yield word.lower(), 1
    def reducer(self, k, v): 
        yield k, sum(v)
if __name__ == '__main__':
    blah = MRWordFreqCount()
    blah.run([(0, "the a the cat in the hat something okay whatever"), (1, "four a the something is up cat is cool hat cat cat")])

# the [1, 1, 1] cat[1, 1] a[1, 1]
# that[1]