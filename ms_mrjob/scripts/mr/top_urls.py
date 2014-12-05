from mrjob.job import MRJob
import string
import itertools
# python top_urls.py < ../../data/user_urls.csv
# user_urls.csv has two kinds of lines
# V, VrootID, UserID
# A, VrootID, URL
# Two step map reduce
# Mapper: For all V lines, 
# 		VrootID, User ID,
# 		For all 'A' lines,
# 		VrootID, URL
# Combiner: For every key, 
# 		User ID, URL
# Reducer: Compute combinations. For every Combination,
# 		Combination, 1
# Combiner2: None, [Combination, sum of occurences]
# Reducer2: Aggregate the output in a list
# 		Sort the list and output top 10 ordered by count

class MRTopUrls(MRJob):

	def mapper_display_input(self,key,value):
		items = value.split(',')
		if items[0] == 'V':
			yield items[1], items[2]	#VID, UID
		else:
			yield items[1], items[2].strip('"')	#VID, URL

	def combiner_user_urls(self,key,value):
		ip_list = list(value)
		vroot_url = ''
		user_list = list()
		# print ip_list[:3]
		for item in ip_list:
			if not item.strip('"').isdigit():
				vroot_url = item 		# if item is not digit, it is the URL element
			else:
				user_list.append(item)
		# print vroot_url, user_list
		for user in user_list:
			yield user, vroot_url 		#yield UID, url

	def reducer_user_urls(self,key,value):
		ip_list = list(value)
		# print ip_list
		comb_list = itertools.combinations(ip_list, 2) #[itertools.combinations(ip_list, i) for i in range(0,len(ip_list)+1)]
		flat_list = comb_list #[item for sublist in comb_list for item in sublist]
		# print type(comb_list)
		for co in flat_list:
			yield co, 1

	def combiner_visits(self,key,value):
		yield None, [key, sum(value)]

	def reducer_init_list(self):
		self.top_urls = list()

	def reducer_top_vids(self,_,uc_pair):
		self.top_urls += list(uc_pair)
		# print self.top_urls

	def reducer_top_vids_final(self):
		s_list = sorted(self.top_urls, key = lambda x:x[1], reverse=True)
		# print s_list[0]
		for i in range(10):
			yield s_list[i][0], s_list[i][1]

	def steps(self):
		return [self.mr(mapper=self.mapper_display_input,
				combiner=self.combiner_user_urls,
				reducer=self.reducer_user_urls),
			self.mr(combiner=self.combiner_visits,
				reducer_init = self.reducer_init_list,
				reducer = self.reducer_top_vids,
				reducer_final = self.reducer_top_vids_final)
		]

if __name__ == '__main__':
    MRTopUrls.run()