# EQUIPE - FRANCISCO FLAVIO CARDOSO GOMES E ISRAEL PORTELA FERREITA

from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations

class Recommender(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_products_by_user,
                 reducer=self.reducer_products_by_user),
             MRStep(mapper=self.combiner_pairs,
                 reducer=self.reducer_pairs),  
             MRStep(reducer=self.reducer2_pairs),
             MRStep(mapper=self.mapper_formatter,
                 reducer=self.reducer_formatter), 
        ]
   
    def mapper_products_by_user(self, _, line):
        (inv, prods, _, _, _) = line.split(';')
        yield inv, prods
    
  
    def reducer_products_by_user(self, inv, prods):
        prods = list(prods)
        yield inv, prods
    
    def combiner_pairs(self, _, prods):
        prods = list(prods)
        prods.sort()
        n=2
        comb=combinations(prods,n)
        for c in comb:
            yield c,1
    

    def reducer_pairs(self, key, values):
        yield key, sum(values)

    
    def reducer2_pairs(self, key, values):
        p1, p2 =key
        yield p1, (sum(values),p2)

    def mapper_formatter(self,key, values):
        yield key, values

    def reducer_formatter(self, key, values):
        values  = list(values)
        values.sort(reverse=True)
        values =  [(p,v) for v, p in values]
        yield key, values   


if __name__ == "__main__":
    Recommender.run()