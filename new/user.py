from master import MRJob
from constants import *

# Example MapReduce job for word counting in some (large, complicated) documents mr-input-1.txt through mr-input-R.txt
class MRWordFreqCount(MRJob):
    def mapper(self, _, line):
        # remove punctuation and make lowercase
        line = line.translate(str.maketrans('', '', punctuation))
        for word in line.split():
            yield word.lower().strip(), 1
            
    def reducer(self, k, v): 
        yield sum(v)


if __name__ == '__main__':
    MRWordFreqCount(M=10, R=3).run() # 10 map tasks, 3 reduce tasks
    print("User's MapReduce job completed! User program can continue now :)")