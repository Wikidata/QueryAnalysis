# lists the top 10 queryPatterns with their respective user agents and some example queries which haven't been classified

import csv
import os
from collections import defaultdict
from pprint import pprint

COMBINATIONS_LIMIT = 10

if not os.path.exists("queryTypeUserAgentCombinations"):
    os.makedirs("queryTypeUserAgentCombinations")

# get top 10 queryTypes from TotalQueryTypeCountSpider.tsv and TotalQueryTypeCountUser.tsv
files = []
files.append("queryType/TotalQueryTypeCountSpider.tsv")
files.append("queryType/TotalQueryTypeCountUser.tsv")

queryTypeUserAgentCombinationsCount = dict()

for file in files:
    print "Working on: " + file
    with open(file) as f:
        for line_no, line in enumerate(csv.reader(f, delimiter="\t")):
            #skip header
            if line_no == 0:
                continue
            if line_no > COMBINATIONS_LIMIT + 1:
                break
            queryTypeUserAgentCombinationsCount[line[0]] = defaultdict(int)


# grep QueryProcessedOpenRDFXX.tsv for these queryTypes and the respective userAgents and create a directory for
# each of these found userAgents and put all found querys in it
files = []
for i in xrange(1, 2):
    files.append("QueryProcessedOpenRDF" + "%02d" % i + ".tsv")


for file in sorted(files):
    print "Working on: " + file
    with open(file) as f:
        for line_no, line in enumerate(csv.reader(f, delimiter="\t")):
            #skip header
            if line_no == 0:
                continue
            #skip invalid ones
            if int(line[0]) != 1:
                continue

            queryType = line[9]
            userAgent = line[11]

            if queryType in queryTypeUserAgentCombinationsCount.keys():
                queryTypeUserAgentCombinationsCount[queryType][userAgent] += 1

for queryType, userAgentCountDict in queryTypeUserAgentCombinationsCount.iteritems():
    for userAgent, userAgentCount in userAgentCountDict.iteritems():

        if not os.path.exists("queryTypeUserAgentCombinations/" + queryType + "/" + str(userAgentCount) + "_" + userAgent.replace('/', 'SLASH') ):
            os.makedirs("queryTypeUserAgentCombinations/" + queryType + "/" + str(userAgentCount) + "_" + userAgent.replace('/', 'SLASH') )



