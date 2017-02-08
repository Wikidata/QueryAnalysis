# lists the top 10 queryPatterns with their respective user agents and some example queries which haven't been classified

import csv
import os
import urlparse
from pprint import pprint


#TODO: filter out those combinations with tool comment!
#--> testen ob die queries richtig geparsed werden!


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
            queryTypeUserAgentCombinationsCount[queryType]['agent'] = file
            queryTypeUserAgentCombinationsCount[queryType]['rank'] = line_no
            queryTypeUserAgentCombinationsCount[queryType]['userAgent'] = dict()


# grep QueryProcessedOpenRDFXX.tsv for these queryTypes and the respective userAgents and create a directory for
# each of these found userAgents and put all found querys in it
files = []
for i in xrange(1, 2):
    files.append("QueryProcessedOpenRDF" + "%02d" % i + ".tsv")

for file in sorted(files):
    print "Working on: " + file
    with open(file) as f:
        reader = csv.DictReader(f, delimiter="\t")
        for line in reader:
            # skip invalid ones
            if int(line['#Valid']) != 1:
                continue

            queryType = line['#QueryType']
            userAgent = line['#user_agent']

            if queryType in queryTypeUserAgentCombinationsCount.keys():
                if userAgent not in queryTypeUserAgentCombinationsCount[queryType]['userAgent'].keys():
                    queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent] = dict()
                    queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent]['count'] = 0
                    queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent]['queries'] = set()
                else:
                    queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent]['count'] += 1
                # search for query
                originalFileLine = line['#original_line(filename_line)']
                originalFile = os.path.basename(originalFileLine.split("_", 1)[0])
                originalLine = int(originalFileLine.split("_", 1)[1])

                originalF = open(originalFile)
                l = originalF.readline(originalLine)

                d = dict(urlparse.parse_qsl(urlparse.urlsplit(l).query))
                if 'query' in d.keys():
                    queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent]['queries'].add(d['query'])




for queryType, userAgentCountDict in queryTypeUserAgentCombinationsCount.iteritems():
    for userAgent, valueDict in userAgentCountDict['userAgent'].iteritems():
        path = "queryTypeUserAgentCombinations/" + queryType + "/" + str(valueDict['count']) + "_" \
               + str(userAgent).replace(
            '/',
            'SLASH')[:100]
        if not os.path.exists(path):
            os.makedirs(path)

        # save userAgent etc. in extra file
        with open(path + "/info.txt", "w") as info_file:
            info_file.write("#UserAgent:\n" + userAgent + "\n#Agent: " + queryTypeUserAgentCombinationsCount[queryType]['agent'] + "\n#Rank: " + str(queryTypeUserAgentCombinationsCount[queryType]['rank']) );


        i = 0
        # save all querys in path
        for query in valueDict['queries']:
            with open(path + "/" + str(i) + ".sparql", "w") as text_file:
                text_file.write(query)
            i += 1
