from mrjob.job import MRJob
from mrjob.step import MRStep

class SortByRating(MRJob):

    # Define all the steps
    def steps(self):
        return [
            MRStep(mapper=self.get_values,
                   reducer=self.count_number_of_rating),
            MRStep(reducer=self.sorting)
        ]

    # Get all the values that are necessary
    def get_values(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, rating

    # Get the number of ratings for each movie
    def count_number_of_rating(self, movie_id, rating):
        yield None, (len(list(rating)), movie_id)

    # Sort the amount of ratings from lowest to highest
    def sorting(self, _, value):
        pairs = sorted(value)
        for single_pair in pairs:
            yield ('Movie ID', single_pair[1]), ('Number of ratings', single_pair[0])


if __name__ == '__main__':
    SortByRating.run()
