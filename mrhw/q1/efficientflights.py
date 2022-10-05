from mrjob.job import MRJob   # MRJob version

# Change the class name!!
class EfficientFlights(MRJob):  #MRJob version
    def mapper_init(self):
        self.cache = {}
    
    def mapper(self, key, line): 
        transaction = line.split(",") # index 7 = country,index 3 =quantity, unit price = index 5 
        if (transaction[0] != "ITIN_ID" and len(transaction) == 8):
            from_airport = transaction[3] #index 3 = from airport
            to_airport = transaction[5] #index 5 = to airport
            num_passengers = float(transaction[7]) # index 7 = number of passengers
            
            self.cache[from_airport] = list(map(sum,zip(self.cache.get(from_airport, [0,0]), [0,num_passengers])))
            self.cache[to_airport] = list(map(sum,zip(self.cache.get(to_airport, [0,0]), [num_passengers,0])))
            
            if (len(self.cache) > 100): 
                for airport in self.cache:
                    yield(airport, self.cache[airport])
                self.cache.clear()

    def mapper_final(self):
       if len(self.cache) > 0:
            for airport in self.cache:
                yield(airport,self.cache[airport])

    def reducer(self, key, values):
        vals = list(values)
   
        val = list(map(sum,zip(*vals))) #equivalent to [for sum(tuple) in zip(*list_of_tuples)]
        yield (key, val)

if __name__ == '__main__':
    EfficientFlights.run()   # MRJob version
