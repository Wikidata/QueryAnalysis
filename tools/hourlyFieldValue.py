import glob
import pprint
import csv
import os
from collections import defaultdict

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

pathBase = "ToolName"

if not os.path.exists(pathBase):
        os.makedirs(pathBase)

monthlyFieldValues = set()

monthlyData = dict()

for i in xrange(1, 32):
	with open("QueryProcessedOpenRDF" + "%02d"%i + ".tsv") as f:
		dailyFieldValues = set()

		dailyData = dict()
		for j in xrange(0, 24):
			dailyData[j] = dict()
			monthlyData[j + 24*(i-1)] = dict()

		reader = csv.DictReader(f, delimiter="\t")
		for line in reader:
			if int(line["#Valid"]) != 1:
				continue

			try:
				hour = int(line["#hour"])
			except ValueError:
				print line["#hour"] + " could not be parsed as integer"
				continue

			if hour in dailyData.keys():
				data = line["#"+pathBase]
				dailyFieldValues.add(data)
				monthlyFieldValues.add(data)
				if data in dailyData[hour].keys():
					dailyData[hour][data] += 1
				else:
					dailyData[hour][data] = 1
				
				monthlyHour = hour + 24*(i-1)
				if data in monthlyData[monthlyHour].keys():
					monthlyData[monthlyHour][data] += 1
				else:
					monthlyData[monthlyHour][data] = 1
			else:
				print hour + " is not in 0-23"
		with open (pathBase + "/" + "Day" + "%02d"%i + "Hourly" + pathBase + ".tsv", "w") as dailyfile:
			writeOut(dailyFieldValues, dailyfile, dailyData)

with open (pathBase + "/" + "TotalHourly" + pathBase + ".tsv", "w") as monthlyfile:
	writeOut(monthlyFieldValues, monthlyfile, monthlyData)
