from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
from math import *

class ContentBasedRecommendation(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_raters_by_movies,
                reducer=self.reducer_raters_by_movies),
            MRStep(mapper=self.mapper_phase_two,
                reducer=self.reducer_phase_two),
            MRStep(mapper=self.mapper_phase_three,
                reducer=self.reducer_phase_three),
            MRStep(mapper=self.mapper_phase_four,
                reducer=self.reducer_phase_four)
        ]

    def mapper_raters_by_movies(self, _, line):
        user, movie, rating, _ = line.split('	')

        yield movie, (user, float(rating))

    def reducer_raters_by_movies(self, key, values):
        items = list(values)

        movie = key
        number_of_raters = len(items)
        for item in items:
            user = item[0]
            rating = item[1]

            yield user, (movie, rating, number_of_raters)

    def mapper_phase_two(self, key, value):
        yield key, value

    def get_combinations(self, items):
        result = []
    
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                m1 = items[i]
                m2 = items[j]

                result.append((m1, m2))

        return result

    def reducer_phase_two(self, key, values):
        items = list(values)

        items.sort()

        tuples = self.get_combinations(items)
        for item in tuples:
            m1, m2 = item
        
            reducer_key = (m1[0], m2[0])
            reduced_value = (m1[1], m1[2], m2[1], m2[2])

            yield reducer_key, reduced_value

    def mapper_phase_three(self, key, value):
        yield key, value

    def reducer_phase_three(self, key, values):
        movie1, movie2 = key

        features1 = []
        features2 = []

        min_rating1 = sys.float_info.max
        max_rating1 = sys.float_info.min
        min_rating2 = sys.float_info.max
        max_rating2 = sys.float_info.min
        avg_rating1 = 0.0
        avg_rating2 = 0.0
        
        items = list(values)
        for item in items:
            rating1, number_of_raters1, rating2, number_of_raters2 = item

            min_rating1 = min(min_rating1, rating1)
            max_rating1 = max(max_rating1, rating1)
            avg_rating1 = avg_rating1 + rating1
            
            min_rating2 = min(min_rating2, rating2)
            max_rating2 = max(max_rating2, rating2)
            avg_rating2 = avg_rating2 + rating2

        first = items[0]
        raters1 = first[1]
        raters2 = first[3]

        avg_rating1 = avg_rating1 / raters1
        avg_rating2 = avg_rating2 / raters2

        features1 = [min_rating1, max_rating1, avg_rating1]
        features2 = [min_rating2, max_rating2, avg_rating2]

        similarity = self.euclidean_distance(features1, features2)

        yield (movie1, movie2), similarity

    def mapper_phase_four(self, key, value):
        movie1, movie2 = key

        yield movie1, (value, movie2)
        yield movie2, (value, movie1)

    def reducer_phase_four(self, key, values):
        items = list(values)
        items.sort(reverse=True)

        yield key, items
        
    def euclidean_distance(self, x, y):
        return 1 / (1 + sqrt(sum(pow(a-b,2) for a, b in zip(x, y))))


if __name__ == "__main__":
    ContentBasedRecommendation.run()