from classes import MRJob

class MRWordFreqCount(MRJob):
    def mapper(self, _, line):
        for word in line.split(' '):
            yield word.lower(), 1
    def reducer(self, k, v): 
        yield k, sum(v)
if __name__ == '__main__':
    blah = MRWordFreqCount()
    blah.run([(0, "cat the four four"), (1, "four cat")])

# the [1, 1, 1] cat[1, 1] a[1, 1]
# that[1]