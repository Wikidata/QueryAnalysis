import glob
import pprint
import csv
import os
from collections import defaultdict

pathBase = "queryType"

if not os.path.exists(pathBase):
	os.makedirs(pathBase)

st = []
for i in xrange(1, 32):
	st.append( "QueryProcessedOpenRDF" + "%02d"%i + ".tsv" )
	
totalQueryTypesUser = defaultdict(int)
totalTotalCountUser = 0

totalQueryTypesSpider = defaultdict(int)
totalTotalCountSpider = 0

header = "query_type\tquery_type_count\tpercentage\n"

for file in sorted( st ):
	print "Working on: " + file
	with open( file ) as f:
		queryTypeCountsUser = defaultdict(int)
		totalCountUser = 0

		queryTypeCountsSpider = defaultdict(int)
		totalCountSpider = 0

		reader = csv.DictReader(f, delimiter="\t")
		for line in reader:

			if int( line['#Valid']) != 1:
				continue

			queryType = line['#QueryType']
			agent = line['#agent_type']

			if agent == "user":
				totalCountUser += 1
				queryTypeCountsUser[queryType] += 1
			if agent == "spider":
				totalCountSpider += 1
				queryTypeCountsSpider[queryType] += 1
		with open (pathBase + "/" + file.split(".")[0] + "QueryTypeCountUser.tsv", "w") as userfile:
			userfile.write(header)
			for k,v in sorted(queryTypeCountsUser.iteritems(), key=lambda (k,v):(v,k), reverse=True):
				totalQueryTypesUser[k] += v	
				percentage = float(v)/totalCountUser*100
				userfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")
		with open (pathBase + "/" + file.split(".")[0] + "QueryTypeCountSpider.tsv", "w") as spiderfile:
			spiderfile.write(header)
			for k,v in sorted(queryTypeCountsSpider.iteritems(), key=lambda (k,v):(v,k), reverse=True):
				totalQueryTypesSpider[k] += v
				percentage = float(v)/totalCountSpider*100
				spiderfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")
		totalTotalCountUser += totalCountUser
		totalTotalCountSpider += totalCountSpider

with open (pathBase + "/TotalQueryTypeCountUser.tsv", "w") as userfile:
	userfile.write(header)
	for k,v in sorted(totalQueryTypesUser.iteritems(), key=lambda (k,v):(v,k), reverse=True):
		percentage = float(v)/totalTotalCountUser*100
		userfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")

with open (pathBase + "/TotalQueryTypeCountSpider.tsv", "w") as spiderfile:
	spiderfile.write(header)
	for k,v in sorted(totalQueryTypesSpider.iteritems(), key=lambda (k,v):(v,k), reverse=True):
        	percentage = float(v)/totalTotalCountSpider*100
		spiderfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")

