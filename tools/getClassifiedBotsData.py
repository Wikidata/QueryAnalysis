import csv
import os
from collections import defaultdict
from pprint import pprint

#X = hour
#Y = count
#Z (stacked X) = ToolName



if not os.path.exists("classifiedBotsData"):
    os.makedirs("classifiedBotsData")


for i in xrange(1, 2):
    with open("../test/test/test/QueryProcessedOpenRDF" + "%02d" % i + ".tsv") as f:
        reader = csv.DictReader(f, delimiter="\t")

        data = dict()

        for line in reader:
            if int(line["#Valid"]) != 1:
                continue

            if line['#hour'] not in data:
                data[line['#hour']] = defaultdict(int)

            data[line['#hour']][line['#ToolName']] += 1

        header = "hour\tToolName\tcount\n"
        with open("classifiedBotsData/"+"%02d" %i + "ClassifiedBotsData.tsv", "w") as outputFile:
            outputFile.write(header)
            for hour, toolNameDict in data.iteritems():
                for toolName in toolNameDict.iterkeys():
                    outputFile.write(str(hour) + "\t" + str(toolName) + "\t" + str(data[hour][toolName]) + "\n")

    with open("classifiedBotsData/TotalClassifiedBotsData.tsv", "w") as outputFile:
        outputFile.write(header)
        for hour, toolNameDict in data.iteritems():
            for toolName in toolNameDict.iterkeys():
                outputFile.write(str(hour) + "\t" + str(toolName) + "\t" + str(data[hour][toolName]) + "\n")
