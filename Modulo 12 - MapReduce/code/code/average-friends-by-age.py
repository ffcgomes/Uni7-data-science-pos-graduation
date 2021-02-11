from mrjob.job import MRJob

class FriendsByAge(MRJob):
    def mapper(self, _, line):
        user, name, age, amount = line.split(',')
        
        # 18-30, 31-45, 46-69

        age = int(age)

        key = None
        if age in range(18, 31):
            key = '18-30'
        elif age in range(31, 46):
            key = '31-45'
        elif age in range(46, 70):
            key = '46-69'

        yield key, float(amount)

    def reducer(self, key, values):
        items = list(values)

        yield key, sum(items)/len(items)

if __name__ == "__main__":
    FriendsByAge.run()