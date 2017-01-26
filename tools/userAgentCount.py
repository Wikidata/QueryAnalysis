import glob
import pprint
import csv
import os
from collections import defaultdict

if not os.path.exists("userAgent"):
	os.makedirs("userAgent")

st = []
for i in xrange(1, 31):
	st.append( "QueryProcessedJena" + "%02d"%i + ".tsv" )
	
totalTriplesUser = defaultdict(int)
totalTotalCountUser = 0

totalTriplesSpider = defaultdict(int)
totalTotalCountSpider = 0

header = "user_agent\tuser_agent_count\tpercentage\n"

for file in sorted( st ):
	print "Working on: " + file
	with open( file ) as f:
		tripleCountsUser = defaultdict(int)
		totalCountUser = 0

		tripleCountsSpider = defaultdict(int)
		totalCountSpider = 0
		for line_no, line in enumerate(csv.reader(f, delimiter="\t")):
			if line_no == 0:
				continue
			if int( line[0]) != 1:
				continue

			triple = line[ 8 ]
			agent = line[ 10 ]
			if agent == "user":
				totalCountUser += 1
				tripleCountsUser[triple] += 1
			if agent == "spider":
				totalCountSpider += 1
				tripleCountsSpider[triple] += 1
		with open ("userAgent/" + file.split(".")[0] + "UserAgentCountUser.tsv", "w") as userfile:
			userfile.write(header)
			for k,v in sorted(tripleCountsUser.iteritems(), key=lambda (k,v):(v,k), reverse=True):
				totalTriplesUser[k] += v	
				percentage = float(v)/totalCountUser*100
				userfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")
		with open ("userAgent/" + file.split(".")[0] + "UserAgentCountSpider.tsv", "w") as spiderfile:
			spiderfile.write(header)
			for k,v in sorted(tripleCountsSpider.iteritems(), key=lambda (k,v):(v,k), reverse=True):
				totalTriplesSpider[k] += v
				percentage = float(v)/totalCountSpider*100
				spiderfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")
		totalTotalCountUser += totalCountUser
		totalTotalCountSpider += totalCountSpider

with open ("userAgent/TotalUserAgentCountUser.tsv", "w") as userfile:
	userfile.write(header)
	for k,v in sorted(totalTriplesUser.iteritems(), key=lambda (k,v):(v,k), reverse=True):
		percentage = float(v)/totalTotalCountUser*100
		userfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")

with open ("userAgent/TotalUserAgentCountSpider.tsv", "w") as spiderfile:
	spiderfile.write(header)
	for k,v in sorted(totalTriplesSpider.iteritems(), key=lambda (k,v):(v,k), reverse=True):
        	percentage = float(v)/totalTotalCountSpider*100
		spiderfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")

