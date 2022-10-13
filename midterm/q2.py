
from mrjob.job import MRJob   # MRJob version

# Change the class name!!
class EfficientOutgoingFlights(MRJob):  #MRJob version
    def mapper_init(self):
        self.cache = {} # now we cache by key = airport.

    def mapper(self, key, line): 
        transaction = line.split(",") 
        #ITIN_ID,YEAR,QUARTER,ORIGIN,ORIGIN_STATE_NM,DEST,DEST_STATE_NM,PASSENGERS
        if (transaction[0] != "ITIN_ID" and len(transaction) == 8):
            from_airport = transaction[3] #index 3 = from airport
            num_passengers = float(transaction[7]) # index 7 = number of passengers
            year = transaction[1] #index 1 = year            
            

#key = airport, value = (2021 outgoing, 2022 outgoing)
# here we aggregate 2021 outgoing and 2022 outgoing by key = from_airport
            if(year == "2021"):
                self.cache[from_airport] = list(map(sum,zip(self.cache.get(from_airport, [0,0]), [num_passengers,0])))

            if(year == "2022"):
                self.cache[from_airport] = list(map(sum,zip(self.cache.get(from_airport, [0,0]), [0,num_passengers])))

        if len(self.cache) > 100:
            for key in self.cache:
                yield(key,self.cache[key])
            self.cache.clear()    
    def mapper_final(self):
        if len(self.cache) > 0:
            for key in self.cache:
                yield(key,self.cache[key])            


    def reducer(self, key, values):
        vals = list(values)
        
        ### values come in as [(2021_passengers,2022_passengers),(2021_pass,2022_pass),(...),...]                  ###
        ### zip will create two lists [[all 2021 values,...],[all 2022 values]] such that### 
        ### each list will have all pasengers for a specific year. Then we sum           ###
        agg = list(map(sum,zip(*vals))) #equivalent to [for sum(tuple) in zip(*list_of_tuples)]
        agg_2021 = agg[0] # total for 2021
        agg_2022 = agg[1] # total for 2022
        total = agg_2022 + agg_2021 # total for airport = key
        difference = agg_2022 - agg_2021 #passenger difference between 2022 and 2021 for airport = key
        yield (key, (total,difference)) 

if __name__ == '__main__':
    EfficientOutgoingFlights.run()   # MRJob version
