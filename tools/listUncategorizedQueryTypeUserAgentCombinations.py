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

topQueryTypes = set()

for file in files:
    print "Working on: " + file
    with open(file) as f:
        for line_no, line in enumerate(csv.reader(f, delimiter="\t")):
            #skip header
            if line_no == 0:
                continue
            if line_no > COMBINATIONS_LIMIT + 1:
                break
            topQueryTypes.add(line[0])

pprint(topQueryTypes)

# grep QueryProcessedOpenRDFXX.tsv for these queryTypes and the respective userAgents and create a directory for


# each of these found userAgents and put all found querys in it
