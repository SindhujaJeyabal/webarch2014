from mrjob.job import MRJob
import string

#python top_pages.py < ../../data/anonymous-msweb.data
# Mapper : Vroot ID, number of visits
# Reducer : vroot ID, sum of visits for all pages with total visits > 400
class MRTopPages(MRJob):
	def mapper_count(self, key, line):
		words = line.split(',')
		if words[0] == 'V':
			out_w = words[1].lower().strip(string.punctuation)
			yield out_w, int(words[2])

	def reducer_sum(self, word, occurrences):
		total = sum(occurrences)
		if total > 400:
			yield word, total

	def steps(self):
		return [self.mr(mapper=self.mapper_count,
			reducer=self.reducer_sum)
		]
if __name__ == '__main__':
	MRTopPages.run()
