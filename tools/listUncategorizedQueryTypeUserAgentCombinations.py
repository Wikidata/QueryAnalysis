# lists the top 10 queryPatterns with their respective user agents and some example queries which haven't been classified

import collections
import csv
import linecache
import os
import urlparse

COMBINATIONS_LIMIT = 10

processedPrefix = "QueryProcessedOpenRDF"
sourcePrefix = "queryCnt"

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

for i in xrange(1, 2):
    print "Working on: %02d" % i
    with open(processedPrefix + "%02d" % i + ".tsv") as p, open(sourcePrefix + "%02d" % i + ".tsv") as s:
        pReader = csv.DictReader(p, delimiter="\t")
        sReader = csv.DictReader(s, delimiter="\t")
        for processed, source in zip(pReader, sReader):
            # skip invalid ones
            if int(processed['#Valid']) != 1:
                continue

            queryType = processed['#QueryType']
            userAgent = source['user_agent']
            toolName = processed['#ToolName']

            if queryType in queryTypeUserAgentCombinationsCount.keys():
                if userAgent not in queryTypeUserAgentCombinationsCount[queryType]['userAgent'].keys():
                    queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent] = dict()
                    queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent]['existingToolNames'] = set()
                    queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent]['count'] = 0
                    queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent]['queries'] = set()
                else:
                    queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent]['count'] += 1

                queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent]['existingToolNames'].add(
                    toolName)

                # search for query
                originalFileLine = processed['#original_line(filename_line)']
                originalFile = os.path.basename(originalFileLine.split("_", 1)[0])
                originalLine = int(originalFileLine.split("_", 1)[1])

                l = linecache.getline(originalFile, originalLine)

                # remove everything after first tab charachter
                l = l.partition("\t")[0]

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
            info_file.write("#UserAgent:\n" + userAgent + "\n#Agent: " + queryTypeUserAgentCombinationsCount[queryType][
                'agent'] + "\n#ExistingToolNames: " + ', '.join(
                queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent][
                    'existingToolNames']) + "\n#Rank: " + str(queryTypeUserAgentCombinationsCount[queryType]['rank']));

        i = 0
        # save all querys in path
        for query in valueDict['queries']:
            with open(path + "/" + str(i) + ".sparql", "w") as text_file:
                text_file.write(query)
            i += 1

combinations = dict()

# rank queryTypeUserAgentCombinations
for queryType, userAgentCountDict in queryTypeUserAgentCombinationsCount.iteritems():
    for userAgent, valueDict in userAgentCountDict['userAgent'].iteritems():
        combinations[valueDict['count']] = queryType + "/" + str(valueDict['count']) + "_" \
                                           + str(userAgent).replace(
            '/',
            'SLASH')[:100]

sortedCombinations = collections.OrderedDict(sorted(combinations.items(), reverse=True))

ranking = str()

for rank, combination in sortedCombinations.iteritems():
    ranking += str(rank) + ":\t\t" + combination + "\n"

with open("queryTypeUserAgentCombinations/ranking.txt", "w") as ranking_file:
    ranking_file.write(ranking)
