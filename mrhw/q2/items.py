from mrjob.job import MRJob   # MRJob version

# Change the class name!!
class Items(MRJob):  #MRJob version
    def mapper(self, key, line): 
        transaction = line.split("\t") # index 1 = stock code,index 3 =quantity, index 7 = country 
        if (transaction[0] != "InvoiceNo" and len(transaction) == 8):
            quantity = abs(float(transaction[3])) #index 3 = quantity
            stock_code = transaction[1] #index 1 = stock code
            country = transaction[7] # index 7 = country
            
            yield(stock_code,(country,quantity))

    def reducer(self, key, values):
        frequency = {}
        total_quantity = 0;
        vals = list(values)
   
        for country,quantity in vals: # aggregate quantity by country
            frequency[country] = frequency.get(country,0) + 1
            total_quantity += quantity
        
        num_countries = len(frequency)
        most_popular_country = max(frequency, key = frequency.get)
        frequency.clear()
        yield(key, (total_quantity, num_countries, most_popular_country))   

if __name__ == '__main__':
    Items.run()   # MRJob version
