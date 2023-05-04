from classes import MRJob
import sys 


class MRWordFreqCount(MRJob):
    def mapper(self, _, line):
        for word in line.split(' '):
            yield word.lower(), 1
            
    def reducer(self, k, v): 
        yield k, sum(v)


if __name__ == '__main__':
    blah = MRWordFreqCount(3, int(sys.argv[1]))
    # 0: 13123 and 1: 13124
    blah.run([(0, "the a the four hello sixsix"), (1, "a four prat prat six hello hello")])
    # blah.run([(0, "the a the the the"), (1, "prat four prat prat")])
# the [1, 1, 1] cat[1, 1] a[1, 1]
# that[1]

# {1: [('the', 1), ('a', 1), ('the', 1)], 0: [('four', 1)]} 0
# {1: [('a', 1)], 0: [('four', 1), ('prat', 1), ('prat', 1)]} 1


"""

{1: [('the', 1), ('a', 1), ('the', 1)], 0: [('four', 1)]} 0
{1: [('a', 1), ('four', 1)], 0: [('four', 1), ('prat', 1), ('prat', 1), ('four', 1)]} 1
"""
"""
four 2 
the 2
a 2
hello 3 
sixsix 1
six 1
prat 2

"""