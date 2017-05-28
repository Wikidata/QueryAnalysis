import csv
import os
import glob
import urlparse

from itertools import izip

processedPrefix = "QueryProcessedOpenRDF"
sourcePrefix = "queryCnt"

TopN = 20

pathBase = "QueryType"
subfolder = pathBase + "/exampleQueries"

topNQueryTypes = []

if not os.path.exists(subfolder):
    os.makedirs(subfolder)

with open(pathBase + "/TotalQueryType_Ranking.tsv") as rankingFile:
    rankingReader = csv.DictReader(rankingFile, delimiter = "\t")
    for line in rankingReader:
        if len(topNQueryTypes) >= TopN:
            break
        topNQueryTypes.append(line["QueryType"])
        
for i in xrange(1, 2):
    print "Working on %02d" % i
    with open(processedPrefix + "%02d" % i + ".tsv") as p, open(sourcePrefix + "%02d" % i + ".tsv") as s:
        pReader = csv.DictReader(p, delimiter="\t")
        sReader = csv.DictReader(s, delimiter="\t")
        
        for processed, source in izip(pReader, sReader):
            queryType = processed["#QueryType"]
            if topNQueryTypes.__contains__(queryType):
                d = dict(urlparse.parse_qsl(urlparse.urlsplit(source['uri_query']).query))
                if queryType.startswith("-"):
                    queryTypeName = queryType.replace("-", "MINUS", 1)
                else:
                    queryTypeName = queryType
                if 'query' in d.keys():
                    with open(subfolder + "/" + queryTypeName + ".example", "w") as queryFile:
                        queryFile.write(d['query'])
                    topNQueryTypes.remove(queryType)
                else:
                    print "Could not find query in uri_query."
            if len(topNQueryTypes) <= 0:
                print "Found examples for all top " + str(TopN) + " query types."
                break

if len(topNQueryTypes) > 0:
    print "Could not find examples for the following query types:"
    for queryType in topNQueryTypes:
        print queryType