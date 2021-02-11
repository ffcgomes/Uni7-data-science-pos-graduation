from mrjob.job import MRJob

class MovingAverage(MRJob):
   
    def mapper(self, _, line):
        # Pattern: GOOG,2004-11-04,184.70
        name, timestamp, value = line.split(',')

        yield name, (timestamp, float(value))

    def reducer(self, key, values):
        items = list(values)
        items.sort()

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