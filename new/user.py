from master import MRJob


# Example MapReduce job for counting word frequencies in some documents mr-input-1.txt through mr-input-4.txt
class MRWordFreqCount(MRJob):
    def mapper(self, _, line):
        for word in line.split(' '):
            yield word.lower(), 1
            
    def reducer(self, k, v): 
        return sum(v)


if __name__ == '__main__':
    MRWordFreqCount(M=4, R=2).run() # 4 map tasks, 2 reduce tasks