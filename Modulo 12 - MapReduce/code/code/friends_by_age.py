from mrjob.job import MRJob

class FriendsByAge(MRJob):
    def mapper(self, _, line):
        user_id, name, age, number = line.split(',')

        yield age, float(number)

    def reducer(self, key, values):
        age = key
        items = list(values)

        avg = sum(items) / len(items)

         yield age, avg

if __name__ == "__main__":
    FriendsByAge.run()