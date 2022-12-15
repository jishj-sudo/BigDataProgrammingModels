from mrjob.job import MRJob
class FBCount(MRJob):
    
    def mapper_init(self):
        self.cache = {} 

    def mapper(self, key, line):
        (left, right) = line.split(" ")
        if int(right) > 500:
            self.cache[left] = self.cache.get(left,0) + 1
     
        if len(self.cache) > 500:
            for key in self.cache:
                yield (key, self.cache[key]) 



    def mapper_final(self):
        if len(self.cache) > 0:
            for key in self.cache:
                yield (key, self.cache[key]) 

    def reducer(self, key, values):
        left = key
        myval = sum(values)
        if myval > 2:
            yield (left, myval + 1) 

if __name__ == '__main__':
    FBCount.run()   
