from mrjob.job import MRJob
import string
# python top_titles.py < ../../data/anonymous-msweb.data
# Two step map reduce
# Mapper : For all attribute lines, 
#				'name', [vrootID, title]
#				For all visits line, 
#				vrootID, number of visits
# Combiner:	For all vRootID lines,
#			vrootId, total visits
#			For the 'name' key lines, 
#			iterate through the value list and output vrootID, title
# Reducer1: 	If value is type int,
#			check for value > 400 and output vrootID, count
#			else output as is
# Reducer2:	Add the values to a dictionary and output titles, count
class MRTopTitles(MRJob):
	def mapper_count(self, key, line):
		words = line.split(',')
		if words[0] == 'A':
			yield 'name', [words[1],words[3].strip(string.punctuation)]
		if words[0] == 'V':
			yield words[1], int(words[2])

	def combiner_count(self, key, value):
		if key != 'name':
			yield key,[sum(value)]
		else:
			ip_list = list(value)
			for vt in ip_list:
				yield vt[0], [vt[1]]

	def reducer_count(self,key,value):
		ip_list = list(value)
		# print ip_list
		for item in ip_list:
			if isinstance(item[0], int):
				if item[0] > 400:
					yield key, [item[0]]
				else:
					pass
			else:
				yield key, [item[0]]

	def reducer_init_dict(self):
		self.title = dict()

	def reducer_dict(self,key,value):
		val_list = list(value)
		self.title[key] = [item for sublist in val_list for item in sublist]

	def reducer_final_dict(self):
		for key in self.title.keys():
			value = self.title[key]
			if len(value) == 2:
				yield value[0], value[1]

	def steps(self):
		return [
			self.mr(mapper=self.mapper_count,
				combiner = self.combiner_count,
				reducer = self.reducer_count),
			self.mr(reducer_init = self.reducer_init_dict,
				reducer = self.reducer_dict,
				reducer_final = self.reducer_final_dict)
		]
if __name__ == '__main__':
    MRTopTitles.run()