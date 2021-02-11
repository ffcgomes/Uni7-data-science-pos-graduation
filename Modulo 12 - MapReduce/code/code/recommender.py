from mrjob.job import MRJob
from mrjob.step import MRStep
import json
from produtos_store import ProdutoStore

class Recommender(MRJob):
    redis = ProdutoStore('192.168.99.100', '6379', 0)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_products_by_user,
                reducer=self.reducer_products_by_user),
            MRStep(mapper=self.mapper_stripes,
                reducer=self.reducer_strips)
        ]

    def mapper_products_by_user(self, _, line):
        (userID, itemID, _) = line.split(',')

        yield userID, itemID

    def reducer_products_by_user(self, userID, values):
        items = list(values)
        yield userID, items
   
    def mapper_stripes(self, userID, items):
        for item in items:
            map = {}
            
            for j in items:
                if j not in map:
                    map[j] = 0
                map[j] = map[j] + 1

            yield item, map

    def reducer_strips(self, item, values):
        stripes = list(values)

        final = {}

        for map in stripes:
            for k, v in map.items():
                if k not in final:
                    final[k] = 0
                final[k] = final[k] + int(v)

        self.redis.save(item, json.dumps(final))

        yield item, final

if __name__ == "__main__":
    Recommender.run()