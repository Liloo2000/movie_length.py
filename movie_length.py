from mrjob.job import MRJob
from mrjob.step import MRStep
import csv


class LongestMovies(MRJob):

	MIN_COUNT = 10
	SHOW_LIMIT = 10


	def movie_title(self, movie_id):
		'''
		Convert from movie id to movie title
		'''
		with open("/root/input/u.item", "r", encoding = "ISO-8859-1") as infile:
			reader = csv.reader(infile, delimiter='|')
			next(reader)
			for line in reader:
				if int(movie_id) == int(line[0]):
					return line[1]


	def steps(self):
		'''
		Pipeline of MapReduce tasks
		'''
		return [
			MRStep(mapper=self.mapper1, reducer=self.reducer1),
			MRStep(mapper=self.mapper2, reducer=self.reducer2)
		]


	def mapper1(self, _, line):
		(user_id, movie_id, rating, timestamp) = line.split('\t')
		yield movie_id, rating
	
	def reducer1(self, movie_id, ratings):
		rating_count = 0
		for r in ratings:
			rating_count += 1
		if rating_count >= self.MIN_COUNT:
			movie_title = self.movie_title(movie_id)
		if movie_title != None:
			yield movie_id, (movie_title, len(movie_title))
	
	def mapper2(self, movie_id, avg_rating):
		yield None, (avg_rating, movie_id)
	
	def reducer2(self, _, values):
		for movie_id, (movie_title, title_len) in sorted(values, key=lambda x: -x[1]):
			i += 1
		if i <= self.SHOW_LIMIT:
			yield movie_id, (movie_title, title_len)


if __name__ == '__main__':
	LongestMovies.run()
