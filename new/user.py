from master import MRJob
from constants import *
import time
    
# Example MapReduce job for mathematical operations in some documents mr-input-1.txt through mr-input-4.txt
class MRWordFreqCount(MRJob):
    def mapper(self, _, line):
        for word in line.split():
            num = int(word.strip())
            yield num % 3, f(num)
            
    def reducer(self, k, v): 
        yield sum(v)


if __name__ == '__main__':
    starttime = time.time()
    MRWordFreqCount(M=12, R=3).run()
    print("User's MapReduce job completed! User program can continue now :)")
    print(f"Took {time.time() - starttime} seconds to finish!")