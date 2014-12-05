from mrjob.job import MRJob
import string

# python top_title_words.py < ../../data/anonymous-msweb.data
# Mapper : tokenize title into words
#				 title word, 1
# Combiner : None, [word, total count]
# Reducer: Initialize a list to store the results
#		Sort the list, outptut the top 10
class MRTopTitleWords(MRJob):
	
	def init_list(self):
		self.ip_list = list()

	def mapper_word_count(self, key, line):
		words = line.split(',')
		if words[0] == 'A':
			title_words = words[3].split()
			for tw in title_words:
				out_w = tw.lower().strip(string.punctuation)
				if out_w != '':
					yield out_w,1
    
	def combiner_word_count(self, key, count):
		yield None, [key, sum(count)]

	def reducer_word_count(self, _, wc_pair):
		self.ip_list += list(wc_pair)

	def reducer_final_count(self):
		s_list = sorted(self.ip_list, key = lambda x:x[1], reverse=True)
		# print s_list
		for i in range(10):
			yield s_list[i][0], s_list[i][1]

	def steps(self):
		return [
			self.mr(mapper = self.mapper_word_count,
				combiner = self.combiner_word_count,
				reducer_init = self.init_list,
				reducer = self.reducer_word_count,
				reducer_final = self.reducer_final_count)
		]
if __name__ == '__main__':
	MRTopTitleWords.run()