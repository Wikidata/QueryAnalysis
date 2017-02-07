# lists the top 10 queryPatterns with their respective user agents and some example queries which haven't been classified

import csv
import os
import urlparse

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
            # skip header
            if line_no == 0:
                continue
            if line_no > COMBINATIONS_LIMIT + 1:
                break
            queryType = line[0]
            queryTypeUserAgentCombinationsCount[queryType] = dict()

# grep QueryProcessedOpenRDFXX.tsv for these queryTypes and the respective userAgents and create a directory for
# each of these found userAgents and put all found querys in it
files = []
for i in xrange(1, 2):
    files.append("QueryProcessedOpenRDF" + "%02d" % i + ".tsv")

for file in sorted(files):
    print "Working on: " + file
    with open(file) as f:
        for line_no, line in enumerate(csv.reader(f, delimiter="\t")):
            # skip header
            if line_no == 0:
                continue
            # skip invalid ones
            if int(line[0]) != 1:
                continue

            queryType = line[10]  # line[9]
            userAgent = line[12]  # line[11]

            if queryType in queryTypeUserAgentCombinationsCount.keys():
                if userAgent not in queryTypeUserAgentCombinationsCount[queryType].keys():
                    queryTypeUserAgentCombinationsCount[queryType][userAgent] = dict()
                    queryTypeUserAgentCombinationsCount[queryType][userAgent]['count'] = 0
                    queryTypeUserAgentCombinationsCount[queryType][userAgent]['queries'] = set()
                else:
                    queryTypeUserAgentCombinationsCount[queryType][userAgent]['count'] += 1
                # search for query
                originalFileLine = line[17]  # line[16]
                originalFile = os.path.basename(originalFileLine.split("_", 1)[0])
                originalLine = int(originalFileLine.split("_", 1)[1])

                originalF = open(originalFile)
                l = originalF.readline(originalLine)

                d = dict(urlparse.parse_qsl(urlparse.urlsplit(l).query))
                if 'query' in d.keys():
                    queryTypeUserAgentCombinationsCount[queryType][userAgent]['queries'].add(d['query'])

for queryType, userAgentCountDict in queryTypeUserAgentCombinationsCount.iteritems():
    for userAgent, valueDict in userAgentCountDict.iteritems():
        path = "queryTypeUserAgentCombinations/" + queryType + "/" + str(valueDict['count']) + "_" + userAgent.replace(
            '/',
            'SLASH')
        if not os.path.exists(path):
            os.makedirs(path)

        i = 0
        # save all querys in path
        for query in valueDict['queries']:
            with open(path + "/" + str(i) + ".sparql", "w") as text_file:
                text_file.write(query)
            i += 1
