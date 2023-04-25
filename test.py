from classes import MRJob

class MRWordFreqCount(MRJob):
    def mapper(self, _, line):
        for word in line.split(' '):
            yield word.lower(), 1

    def reducer(self, word, counts):
        yield word, sum(counts)
    
if __name__ == '__main__':
    blah = MRWordFreqCount()
    blah.run([(0, "cat a that the"), (1, "the cat a the")])