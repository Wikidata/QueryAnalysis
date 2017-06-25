import csv
import os

from itertools import izip


def writeOut(fieldValues, file, dictionary):
    header = "hour"
    for field in sorted(fieldValues):
        header += "\t" + field
    file.write(header + "\n")
    for j in sorted(dictionary.keys()):
        line = str(j)
        for field in sorted(fieldValues):
            if field in dictionary[j].keys():
                line += "\t" + str(dictionary[j][field])
            else:
                line += "\t0"
        file.write(line + "\n")


metrics = ['ToolName']
for metric in metrics:
    print "Working on " + metric

    pathBase = metric

    if not os.path.exists(pathBase):
        os.makedirs(pathBase)

    monthlyFieldValues = set()

    monthlyData = dict()

    for i in xrange(1, 2):
        print "\tWorking on %02d" % i
        with open("QueryProcessedOpenRDF" + "%02d" % i + ".tsv") as p, open("queryCnt" + "%02d" % i + ".tsv") as s:
            dailyFieldValues = set()

            dailyData = dict()
            for j in xrange(0, 24):
                dailyData[j] = dict()
                monthlyData[j + 24 * (i - 1)] = dict()

            pReader = csv.DictReader(p, delimiter="\t")
            sReader = csv.DictReader(s, delimiter="\t")
            for processed, source in izip(pReader, sReader):
                if int(processed["#Valid"]) != 1:
                    continue

                # onlfy for "user queries"
                if processed['#ToolName'] == '0':
                    try:
                        hour = int(source["hour"])
                    except ValueError:
                        print source["hour"] + " could not be parsed as integer"
                        continue

                    if hour in dailyData.keys():
                        data = processed["#" + pathBase]
                        dailyFieldValues.add(data)
                        monthlyFieldValues.add(data)
                        if data in dailyData[hour].keys():
                            dailyData[hour][data] += 1
                        else:
                            dailyData[hour][data] = 1

                        monthlyHour = hour + 24 * (i - 1)
                        if data in monthlyData[monthlyHour].keys():
                            monthlyData[monthlyHour][data] += 1
                        else:
                            monthlyData[monthlyHour][data] = 1
                    else:
                        print hour + " is not in 0-23"
            with open(pathBase + "/" + "Day" + "%02d" % i + "Hourly" + pathBase + ".tsv", "w") as dailyfile:
                writeOut(dailyFieldValues, dailyfile, dailyData)

    with open(pathBase + "/" + "TotalHourly" + pathBase + ".tsv", "w") as monthlyfile:
        writeOut(monthlyFieldValues, monthlyfile, monthlyData)
