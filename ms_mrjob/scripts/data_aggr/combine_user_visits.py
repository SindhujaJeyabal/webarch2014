import sys

current_user = None

for line in sys.stdin:
	words = line.split(',')
	if words[0] == 'C':
		current_user = words[1]
	elif words[0] == 'V':
		pages = words[1]
		print "%s,%s" %(pages, current_user)
	else:
		pass