from classes import MRJob

class MRWordFreqCount(MRJob):
    def mapper(self, _, line):
        for word in line.split(' '):
            yield word.lower(), 1

    def reducer(self, word, counts):
        yield word, sum(counts)
    
if __name__ == '__main__':
    blah = MRWordFreqCount()
    blah.run([(0, "cat a that the four"), (1, "the cat a the four"), (2, "the cat cat a that four")])

# the [1, 1, 1] cat[1, 1] a[1, 1]
# that[1]