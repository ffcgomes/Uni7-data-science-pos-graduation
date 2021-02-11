from mrjob.job import MRJob

class CustomerOrders(MRJob):
    def mapper(self, _, line):
        customer, _, amount = line.split(',')
        
        yield customer, float(amount)

    def reducer(self, key, values):
       items = list(values) 
       yield key, (min(items), max(items), sum(items) / len(items))

if __name__ == "__main__":
    CustomerOrders.run()
