"""
Alunos:
- Allan Reffson Granja Lima
- Jean Carlos Maia e Silva
"""

import sys
from datetime import datetime
from itertools import combinations

from mrjob.job import MRJob
from mrjob.step import MRStep


class MBARetail(MRJob):

    def steps(self):
        return [
            # Fase 1
            MRStep(mapper=self.mapper_products,
                   reducer=self.reducer_products,
                   ),
            # Fase 2
            MRStep(mapper=self.mapper_pairs,
                   combiner=self.combiner_pairs,
                   reducer=self.reducer_pairs,
                   ),
            # Fase 3
            MRStep(mapper=self.mapper_formatter,
                   reducer=self.reducer_formatter,
                   ),
        ]

    def mapper_products(self, _, line):
        invoice, product, _, _, _ = line.split(';')
        yield invoice, product

    def reducer_products(self, key, values):
        yield key, list(values)

    def mapper_pairs(self, key, values):
        values = list(values)
        values.sort()
        p = 2
        comb = combinations(values, p)
        for c in comb:
            yield c, 1

    def combiner_pairs(self, key, values):
        yield key, sum(values)

    def reducer_pairs(self, key, values):
        p1, p2 = key
        yield p1, (sum(values), p2)

    def mapper_formatter(self, key, values):
        yield key, values

    def reducer_formatter(self, key, values):
        values = list(values)
        values.sort(reverse=True)
        values = [(p, v) for v, p in values]
        yield key, values


if __name__ == '__main__':
    start_time = datetime.now()
    MBARetail.run()
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    sys.stderr.write(str(elapsed_time))
