import sys
import string
current_user = None

for line in sys.stdin:
	words = line.split(',')
	if words[0] == 'C':
		current_user = words[1]
	elif words[0] == 'V':
		pages = words[1]
		print 'V,%s,%s' %(pages, current_user)
	elif words[0] == 'A':
		print 'A,%s,%s' %(words[1],words[4].strip('\n'))