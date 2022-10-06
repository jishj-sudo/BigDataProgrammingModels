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
        log_of_countries = {}
        total_quantity = 0;
        vals = list(values)
   
        for country,quantity in vals: # aggregate quantity by country
            log_of_countries[country] = log_of_countries.get(country,0) + quantity
            total_quantity += quantity
        
        num_countries = len(log_of_countries)
        most_popular_country = max(log_of_countries, key = log_of_countries.get)
        log_of_countries.clear()
        yield(key, (total_quantity, num_countries, most_popular_country))   

if __name__ == '__main__':
    Items.run()   # MRJob version
