from mrjob.job import MRJob
from mrjob.step import MRStep

class MostUsedWord(MRJob):
    def mapper_count(self, _, line):
        words = line.split()
        for word in words:
            yield word, 1

    def reducer_count(self, key, values):
        yield None, (sum(values), key)

    def reducer_top(self, key, values):
        items = list(values)
        items.sort()

        top = items[len(items) - 1]

        yield top[0], top[1]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_count,
                reducer=self.reducer_count),
            MRStep(reducer=self.reducer_top)
        ]

if __name__ == "__main__":
    MostUsedWord.run()