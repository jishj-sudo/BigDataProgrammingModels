from mrjob.job import MRJob   # MRJob version

# Change the class name!!
class WordLengthCount0(MRJob):  #MRJob version
    def mapper(self, key, line):
        words = line.split()
        for w in words:
            yield (len(w), 1) #key is now length of word.

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    WordLengthCount0.run()   # MRJob version
