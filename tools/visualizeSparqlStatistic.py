from __future__ import division

import csv
from collections import defaultdict

import os
import sys

workingDir = sys.argv[1]
os.chdir(workingDir)

notStatisticNames = ['#Valid', '#ToolName', '#ToolVersion', '#StringLengthWithComments', '#QuerySize',
                     '#VariableCountHead',
                     '#VariableCountPattern', '#TripleCountWithService', '#TripleCountNoService', '#QueryType', '#QIDs',
                     '#original_line(filename_line)', '#ExampleQueryStringComparison', '#ExampleQueryParsedComparison']

statistic = defaultdict(int)
totalCount = 0

for i in xrange(1, 6):
    with open("QueryProcessedOpenRDF%02d" % i + ".tsv") as file:
        reader = csv.DictReader(file, delimiter="\t")
        for line in reader:
            if int(line["#Valid"]) != 1:
                continue
            if (line["#ToolName"] != "USER"):
                continue
            totalCount += 1
            for featureName in line:
                if featureName in notStatisticNames:
                    continue
                if line[featureName] is not "0":
                    statistic[featureName] += 1  # int(line[featureName])

for featureName, featureCount in sorted(statistic.iteritems()):
    print('{:<28} {:>8}/{:<8} {:>5}%'.format(featureName, featureCount, totalCount,
                                             round(featureCount / totalCount * 100, 2)))
