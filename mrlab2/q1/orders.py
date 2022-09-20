from mrjob.job import MRJob   # MRJob version

# Change the class name!!
class Orders(MRJob):  #MRJob version
    def mapper(self, key, line): 
        transaction = line.split("\t") # index 7 = country,index 3 =quantity, unit price = index 5 
        if(transaction[0] == 'InvoiceNo'):
            yield("United Kingdom", 0)
        country = transaction[7]
        quantity = float(transaction[3])
        unit_price = float(transaction[5])        

        cost = quantity * unit_price
        yield (country, cost)

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    Orders.run()   # MRJob version
