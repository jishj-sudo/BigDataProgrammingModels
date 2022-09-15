from mrjob.job import MRJob   # MRJob version

# Change the class name!!
class WordCount(MRJob):  #MRJob version
    def mapper(self, key, line):
        counter = {}
        words = line.split()
        for w in words:
            if (len(w) > 2): # only include words greater than length 2    
                counter[len(w)] = counter.get(len(w),0)                         
        for k,v in counter:
            if(v>1):
                yield (len(k), ) 

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    WordCount.run()   # MRJob version
