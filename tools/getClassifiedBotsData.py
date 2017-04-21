import csv
from collections import defaultdict

import os
import sys

# X = hour
# Y = count
# Z (stacked X) = ToolName


workingDir = sys.argv[1]
os.chdir(workingDir)

metricName = "ToolName"

if not os.path.exists("classifiedBotsData"):
    os.makedirs("classifiedBotsData")

for i in xrange(1, 2):
    with open("QueryProcessedOpenRDF" + "%02d" % i + ".tsv") as f:
        reader = csv.DictReader(f, delimiter="\t")

        data = dict()

        for line in reader:
            if int(line["#Valid"]) != 1:
                continue

            if line['#hour'] not in data:
                data[line['#hour']] = defaultdict(int)

            data[line['#hour']][line['#' + metricName]] += 1

        header = "hour\t" + metricName + "\tcount\n"
        with open("classifiedBotsData/" + "%02d" % i + "ClassifiedBotsData.tsv", "w") as outputFile:
            outputFile.write(header)
            for hour, metricDict in data.iteritems():
                for metric in metricDict.iterkeys():
                    outputFile.write(str(hour) + "\t" + str(metric) + "\t" + str(data[hour][metric]) + "\n")

    with open("classifiedBotsData/TotalClassifiedBotsData.tsv", "w") as outputFile:
        outputFile.write(header)
        for hour, metricDict in data.iteritems():
            for metric in metricDict.iterkeys():
                outputFile.write(str(hour) + "\t" + str(metric) + "\t" + str(data[hour][metric]) + "\n")
