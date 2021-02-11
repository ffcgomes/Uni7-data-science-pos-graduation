from mrjob.job import MRJob

class Temperature(MRJob):
    def mapper(self, _, line):
        # EZE00100082,18000105,TMAX,-40,,,E,
        location, timestamp, metric, value, _, _, _, _ = line.split(',')

        if metric == 'TMIN' or metric == 'TMAX':
          yield location, float(value)

    def reducer(self, key, values):
        items = list(values)
        yield key, {
            'min' : min(items),
            'max' : max(items)
        }

if __name__ == "__main__":
    Temperature.run()