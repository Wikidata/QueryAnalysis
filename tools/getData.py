import glob
import pprint
import csv
import os
from collections import defaultdict
from dateutil import parser
from pprint import pprint

outputDirectory = "dayTriple/"

if not os.path.exists(outputDirectory):
    os.makedirs(outputDirectory)

st = []
for i in xrange(1, 2):
    st.append("QueryProcessedJena" + "%02d" % i + ".tsv")

data = dict()
totalData = dict()

totalData["user"] = dict()
totalData["spider"] = dict()
data["user"] = dict()
data["spider"] = dict()

day = False

#metricName = "tripleCount"
#metricRow = 5


metricName = "stringLengthNoComments"
metricRow = 2

if day:
    timeName = "Day"
    start = 1
    end = 31
else:
    timeName = "Hour"
    start = 0
    end = 24

header = timeName + "\t" + metricName + "\thow_many\n"

for i in range(start, end):
    data["user"][i] = defaultdict(int)
    data["spider"][i] = defaultdict(int)
    totalData["user"][i] = defaultdict(int)
    totalData["spider"][i] = defaultdict(int)

for file in sorted(st):
    print "Working on: " + file
    with open(file) as f:
        for line_no, line in enumerate(csv.reader(f, delimiter="\t")):
            if line_no == 0:
                continue
            if int(line[0]) <= 0:
                continue

            metric = int(line[metricRow])

            try:
                dt = parser.parse(line[9])
                if day:
                    time = dt.day
                else:
                    time = dt.hour

                agent = line[10]

                data[agent][time][metric] += 1
            except ValueError:
                print "Unparsable date: " + line[9]

# pprint(data)

for i in range(0, 2):
    if i == 0:
        agent = "user"
    else:
        agent = "spider"
    with open(outputDirectory + agent + timeName + metricName.title() + ".tsv", "w") as outputFile:
        outputFile.write(header)
        for time, defaultdict in data[agent].iteritems():
            for metric, how_many in defaultdict.iteritems():
                outputFile.write(str(time) + "\t" + str(metric) + "\t" + str(how_many) + "\n")

for i in range(0, 2):
    if i == 0:
        agent = "user"
    else:
        agent = "spider"

    for time, defaultdict in data[agent].iteritems():
        for key, value in defaultdict.iteritems():
            totalData[agent][time][key] += value

# pprint(totalData)


for i in range(0, 2):
    if i == 0:
        agent = "user"
    else:
        agent = "spider"
    with open(outputDirectory + agent + timeName + "Total" + metricName.title() + ".tsv", "w") as outputFile:
        outputFile.write(header)
        for time, defaultdict in totalData[agent].iteritems():
            for metric, how_many in defaultdict.iteritems():
                outputFile.write(str(time) + "\t" + str(metric) + "\t" + str(how_many) + "\n")