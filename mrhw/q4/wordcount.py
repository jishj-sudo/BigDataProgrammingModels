from mrjob.job import MRJob   # MRJob version

# Change the class name!!
class WordCount(MRJob):  #MRJob version
    def mapper_init(self):
        self.mapper_cache = {}

    def mapper(self, key, line): 
        transaction = line.split()
        for word in transaction:
            truncated_word = word.replace(",","").replace(".","").replace("\u201d","").replace("\u201c","").replace("?","").replace("!","").replace("(","").replace(")","").replace(";","").replace(":","")
            if truncated_word.isalpha(): 
                self.mapper_cache[truncated_word] = self.mapper_cache.get(truncated_word,0) + 1
        if len(self.mapper_cache) > 100:
            for word,count in self.mapper_cache.items():
                yield(word,count)
            self.mapper_cache.clear()

    def mapper_final(self):
        if len(self.mapper_cache) > 0:
            for word,count in self.mapper_cache.items():
                yield(word,count)
            self.mapper_cache.clear()


    def reducer_init(self):
        self.greatest_sum = 0
        self.most_frequent = "nothing yet"

    def reducer(self, key, values):
        agg_sum = sum(values)
        if self.greatest_sum <= agg_sum:
            self.greatest_sum = agg_sum
            self.most_frequent = key
        yield (key, agg_sum)

    def reducer_final(self):
        yield("MostFrequent", self.most_frequent)
        
if __name__ == '__main__':
    WordCount.run()   # MRJob version
