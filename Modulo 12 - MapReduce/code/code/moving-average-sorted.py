from mrjob.job import MRJob
from mrjob.step import MRStep

class MovingAverage(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_sort),
            MRStep(reducer=self.reducer)
        ]
   
    def mapper(self, _, line):
        # Pattern: GOOG,2004-11-04,184.70
        name, timestamp, value = line.split(',')

        yield (name, timestamp), float(value)

    def reducer_sort(self, key, values):
        items = list(values)

        for item in items:
            yield key[0], (key[1], item)

    def reducer(self, key, values):
        items = list(values)

        w = 3
        total = 0.0
        for i in range(len(items)):
            item = items[i]
            total += item[1]
            trash = None

            if i >= w:
                total -= items[i - w][1]
                trash = items[i - w][1]
            
            quociente = min(i + 1, w)
            ma = total / quociente

            yield key, (item[0], item[1], ma, quociente, trash)

if __name__ == "__main__":
    MovingAverage.run()