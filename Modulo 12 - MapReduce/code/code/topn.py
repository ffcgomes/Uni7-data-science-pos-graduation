from mrjob.job import MRJob
from mrjob.step import MRStep

class TopN(MRJob):
    top = []
    N = 5

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer_init=self.reducer_init),
            MRStep(reducer=self.reducer),
        ]

    def mapper(self, _, line):
        weight, id, name = line.split(',')

        self.top.append((float(weight), id))
        if len(self.top) > self.N:
            self.top.sort()
            self.top.pop(0)

    def reducer_init(self):
        for i in self.top:
            yield None, i

    def reducer(self, key, values):
        items = list(values)

        # final = []

        # for value in values:
        #     self.final.append(value)
        #     if len(self.final) > self.N:
        #         self.final.sort()
        #         self.final.pop(0)

        items.sort(reverse=True)
        final = items[:self.N]

        for value in final:
            yield value[0], value[1]

if __name__ == "__main__":
    TopN.run()