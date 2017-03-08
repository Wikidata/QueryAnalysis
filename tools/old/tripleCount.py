import csv
import os
from collections import defaultdict

if not os.path.exists("tripleCount"):
    os.makedirs("tripleCount")

st = []
for i in xrange(1, 31):
    st.append("QueryProcessedJena" + "%02d" % i + ".tsv")

totalTriplesUser = defaultdict(int)
totalTotalCountUser = 0

totalTriplesSpider = defaultdict(int)
totalTotalCountSpider = 0

header = "triple_count\ttriple_count_count\tpercentage\n"

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
            totalCountUser += 1
            totalCountSpider += 1

            triple = int(line[5])
            agent = line[10]
            if agent == "user":
                tripleCountsUser[triple] += 1
            if agent == "spider":
                tripleCountsSpider[triple] += 1
        with open("tripleCount/" + file.split(".")[0] + "TripleCountUser.tsv", "w") as userfile:
            userfile.write(header)
            for k, v in tripleCountsUser.iteritems():
                totalTriplesUser[k] += v
                percentage = float(v) / totalCountUser * 100
                userfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")
        with open("tripleCount/" + file.split(".")[0] + "TripleCountSpider.tsv", "w") as spiderfile:
            spiderfile.write(header)
            for k, v in tripleCountsSpider.iteritems():
                totalTriplesSpider[k] += v
                percentage = float(v) / totalCountSpider * 100
                spiderfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")
        totalTotalCountUser += totalCountUser
        totalTotalCountSpider += totalCountSpider

with open("tripleCount/TotalTripleCountUser.tsv", "w") as userfile:
    userfile.write(header)
    for k, v in totalTriplesUser.iteritems():
        percentage = float(v) / totalTotalCountUser * 100
        userfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")

with open("tripleCount/TotalTripleCountSpider.tsv", "w") as spiderfile:
    spiderfile.write(header)
    for k, v in totalTriplesSpider.iteritems():
        percentage = float(v) / totalTotalCountSpider * 100
        spiderfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")
