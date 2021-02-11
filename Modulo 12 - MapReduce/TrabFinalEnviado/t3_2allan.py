"""
Alunos:
- Allan Reffson Granja Lima
- Jean Carlos Maia e Silva
"""

import sys
from datetime import datetime

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
            MRStep(mapper=self.mapper_stripes,
                   reducer=self.reducer_stripes,
                   ),
            # Fase 3
            MRStep(mapper=self.mapper_formatter,
                   reducer=self.reducer_formatter,
                   ),
        ]

    def mapper_products(self, _, line):
        _, product, _, client, _ = line.split(';')

        yield client, product

    def reducer_products(self, key, values):
        values = list(values)

        yield key, values

    def mapper_stripes(self, key, values):
        products = list(values)
        for prod in products:
            m = {}
            for prod_stripe in products:
                if prod_stripe not in m:
                    m[prod_stripe] = 0

                m[prod_stripe] += 1

            yield prod, m

    def reducer_stripes(self, key, values):
        products_stripes = list(values)
        final = {}
        for ps in products_stripes:
            for k, v in ps.items():
                if k not in final:
                    final[k] = 0

                final[k] += v
        final = [(v, k) for k, v in final.items()]

        yield key, final

    def mapper_formatter(self, key, values):
        values.sort(reverse=True)
        values = [(v, k) for k, v in values]

        yield key, values

    def reducer_formatter(self, key, values):
        values = list(values)[0]

        yield key, values[:5]


if __name__ == '__main__':
    start_time = datetime.now()
    MBARetail.run()
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    sys.stderr.write(str(elapsed_time))
