from mrjob.job import MRJob
import string
# python top_users.py < ../../data/user_visits.csv
# Mapper : User ID, 1
# Reducer : User ID, total visits for all users with visits > 20

class MRTopUsers(MRJob):
	def mapper_top_users(self, key, line):
		words = line.split(',')
		yield words[1].strip(string.punctuation), 1

	def reducer_top_users(self,user,count):
		total = sum(count)
		if total > 20:
			yield user, total

	def steps(self):
		return [
			self.mr(mapper=self.mapper_top_users,
			reducer=self.reducer_top_users)
		]
if __name__ == '__main__':
    MRTopUsers.run()