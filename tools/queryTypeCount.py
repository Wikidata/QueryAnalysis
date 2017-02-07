import glob
from pprint import pprint
import csv
import os
from collections import defaultdict

if not os.path.exists("queryType"):
    os.makedirs("queryType")

st = []
for i in xrange(1, 2):
    st.append("QueryProcessedOpenRDF" + "%02d" % i + ".tsv")

totalTriplesUser = defaultdict(int)
totalTotalCountUser = 0

totalTriplesSpider = defaultdict(int)
totalTotalCountSpider = 0

header = "query_type\tquery_type_count\tpercentage\n"

for file in sorted(st):
    print "Working on: " + file
    with open(file) as f:
        tripleCountsUser = defaultdict(int)
        totalCountUser = 0

        tripleCountsSpider = defaultdict(int)
        totalCountSpider = 0
        for line_no, line in enumerate(csv.reader(f, delimiter="\t")):
            if line_no == 0:
                continue
            if int(line[0]) != 1:
                continue

            triple = line[9]
            agent = line[13]

            if agent == "user":
                totalCountUser += 1
                tripleCountsUser[triple] += 1
            if agent == "spider":
                totalCountSpider += 1
                tripleCountsSpider[triple] += 1
        with open("queryType/" + file.split(".")[0] + "QueryTypeCountUser.tsv", "w") as userfile:
            userfile.write(header)
            for k, v in sorted(tripleCountsUser.iteritems(), key=lambda (k, v): (v, k), reverse=True):
                totalTriplesUser[k] += v
                percentage = float(v) / totalCountUser * 100
                userfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")
        with open("queryType/" + file.split(".")[0] + "QueryTypeCountSpider.tsv", "w") as spiderfile:
            spiderfile.write(header)
            for k, v in sorted(tripleCountsSpider.iteritems(), key=lambda (k, v): (v, k), reverse=True):
                totalTriplesSpider[k] += v
                percentage = float(v) / totalCountSpider * 100
                spiderfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")
        totalTotalCountUser += totalCountUser
        totalTotalCountSpider += totalCountSpider

with open("queryType/TotalQueryTypeCountUser.tsv", "w") as userfile:
    userfile.write(header)
    for k, v in sorted(totalTriplesUser.iteritems(), key=lambda (k, v): (v, k), reverse=True):
        percentage = float(v) / totalTotalCountUser * 100
        userfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")

with open("queryType/TotalQueryTypeCountSpider.tsv", "w") as spiderfile:
    spiderfile.write(header)
    for k, v in sorted(totalTriplesSpider.iteritems(), key=lambda (k, v): (v, k), reverse=True):
        percentage = float(v) / totalTotalCountSpider * 100
        spiderfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")
