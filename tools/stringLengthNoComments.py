import glob
import pprint
import csv
import os
from collections import defaultdict

if not os.path.exists("stringLengthNoComments"):
	os.makedirs("stringLengthNoComments")

st = []
for i in xrange(1, 31):
	st.append( "QueryProcessedJena" + "%02d"%i + ".tsv" )
	
totalStringLengthNoCommentsUser = defaultdict(int)
totalTotalCountUser = 0

totalStringLengthNoCommentsSpider = defaultdict(int)
totalTotalCountSpider = 0

header = "StringLengthNoComments\tcount\tpercentage\n"

for file in sorted( st ):
	print "Working on: " + file
	with open( file ) as f:
		stringLengthNoCommentsCountsUser = defaultdict(int)
		totalCountUser = 0

		stringLengthNoCommentsCountsSpider = defaultdict(int)
		totalCountSpider = 0
		for line_no, line in enumerate(csv.reader(f, delimiter="\t")):
			if line_no == 0:
				continue
			if int( line[0]) != 1:
				continue
			totalCountUser += 1
			totalCountSpider += 1
			stringLengthNoComments = int( line[ 2 ] )
			agent = line[ 10 ]
			if agent == "user":
				stringLengthNoCommentsCountsUser[stringLengthNoComments] += 1
			if agent == "spider":
				stringLengthNoCommentsCountsSpider[stringLengthNoComments] += 1
		with open ("stringLengthNoComments/" + file.split(".")[0] + "StringLengthNoCommentsUser.tsv", "w") as userfile:
			userfile.write(header)
			for k,v in stringLengthNoCommentsCountsUser.iteritems():
				totalStringLengthNoCommentsUser[k] += v	
				percentage = float(v)/totalCountUser*100
				userfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")
		with open ("stringLengthNoComments/" + file.split(".")[0] + "StringLengthNoCommentsSpider.tsv", "w") as spiderfile:
			spiderfile.write(header)
			for k,v in stringLengthNoCommentsCountsSpider.iteritems():
				totalStringLengthNoCommentsSpider[k] += v
				percentage = float(v)/totalCountSpider*100
				spiderfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")
		totalTotalCountUser += totalCountUser
		totalTotalCountSpider += totalCountSpider

with open ("stringLengthNoComments/TotalStringLengthNoCommentsCountUser.tsv", "w") as userfile:
	userfile.write(header)
	for k,v in totalStringLengthNoCommentsUser.iteritems():
		percentage = float(v)/totalTotalCountUser*100
		userfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")

with open ("stringLengthNoComments/TotalStringLengthNoCommentsCountSpider.tsv", "w") as spiderfile:
	spiderfile.write(header)
	for k,v in totalStringLengthNoCommentsSpider.iteritems():
        	percentage = float(v)/totalTotalCountSpider*100
		spiderfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")

