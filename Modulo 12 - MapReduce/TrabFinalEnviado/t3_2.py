"""
Alunos:
- Francisco Flávio Cardoso Gomes
- Israel Portela Ferreira
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

class Recommend(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_prodClient,
                   reducer=self.reducer_prod,
                   ),
            MRStep(mapper=self.mapper_stripes,
                   reducer=self.reducer_stripes,
                   ),
            MRStep(mapper=self.mapper_sort,
                   reducer=self.reducer_sort_and_filter,
                   ),
        ]
    def mapper_prodClient(self, _, line):
        _, prods, _, client, _ = line.split(';')
        yield client, prods
    
    def reducer_prod(self, client, prods):
        prods = list(prods)
        yield client, prods

    def mapper_stripes(self, client, prods):
        prods = list(prods)
        for prod in prods:
            m = {}
            for prod_stripe in prods:
                if prod_stripe not in m:
                    m[prod_stripe] = 0
                m[prod_stripe] += 1
            yield prod, m
    
    def reducer_stripes(self, prod, m):
        prods = list(m)
        recommend = {}
        for ps in prods:
            for k, v in ps.items():
                if k not in recommend:
                    recommend[k] = 0
                recommend[k] += v
        recommend = [(v, k) for k, v in recommend.items()]
        yield prod, recommend
    
    def mapper_sort(self, prod, recommend):
        recommend.sort(reverse=True)
        recommend = [(v, k) for k, v in recommend]
        yield prod, recommend

    def reducer_sort_and_filter(self,prod, recommend):
        recommend = list(recommend)[0]
        yield prod, recommend[:5]

if __name__ == '__main__':
    Recommend.run()
