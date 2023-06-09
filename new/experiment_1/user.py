from master import MRJob
from constants import *
import time

# Example MapReduce job for word counting in some (large, complicated) documents mr-input-1.txt through mr-input-R.txt
class MRWordFreqCount(MRJob):
    def mapper(self, _, line):
        # remove punctuation and make lowercase
        line = line.translate(str.maketrans('', '', punctuation))
        for word in line.split():
            yield word.lower(), 1
            
    def reducer(self, k, v): 
        yield sum(v)


if __name__ == '__main__':
    starttime = time.time()
    MRWordFreqCount(M=25, R=5).run()
    print("User's MapReduce job completed! User program can continue now :)")
    print(f"Took {time.time() - starttime} seconds to finish!")