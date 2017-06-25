import csv
import os
from collections import defaultdict

from itertools import izip


def listToString(list):
	returnString = ""
	for entry in list:
		returnString += entry + ","
	return returnString[:-1]


processedPrefix = "QueryProcessedOpenRDF"
sourcePrefix = "queryCnt"

# Set if you only want to rank ID-Combinations with a specific number of IDs, default ranks all.
idCombinations = 0

metrics = ['PIDs']
for metric in metrics:
	print "Working on " + metric

	pathBase = metric

	if not os.path.exists(pathBase):
		os.makedirs(pathBase)

	header = pathBase + "\t" + pathBase + "_count\tpercentage\n"

	totalCount = 0
	totalMetricCounts = defaultdict(int)

	for i in xrange(1, 2):
		print "Working on: %02d" % i
		with open(processedPrefix + "%02d" % i + ".tsv") as p, open(sourcePrefix + "%02d" % i + ".tsv") as s:

			dailyCount = 0
			dailyMetricCounts = defaultdict(int)

			pReader = csv.DictReader(p, delimiter="\t")
			sReader = csv.DictReader(s, delimiter="\t")
			for processed, source in izip(pReader, sReader):
				if int(processed['#Valid']) != 1:
					continue

				key = processed['#' + metric]

				# Sort the entries in PIDs and QIDs to even out the count 
				if metric == "PIDs" or metric == "QIDs":
					keys_array = sorted(key.split(","))
					if idCombinations == 0 or len(keys_array) == idCombinations:
						key = listToString(keys_array)
					else:
						continue

				dailyCount += 1
				dailyMetricCounts[key] += 1

			totalCount += dailyCount

		with open(pathBase + "/Day" + "%02d" % i + pathBase + "_Ranking.tsv", "w") as dailyfile:
			dailyfile.write(header)
			for k, v in sorted(dailyMetricCounts.iteritems(), key=lambda (k, v): (v, k), reverse=True):
				totalMetricCounts[k] += v
				percentage = float(v) / dailyCount * 100
				dailyfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")

	with open(pathBase + "/" + "Total" + pathBase + "_Ranking.tsv", "w") as totalfile:
		totalfile.write(header)
		for k, v in sorted(totalMetricCounts.iteritems(), key=lambda (k, v): (v, k), reverse=True):
			percentage = float(v) / totalCount * 100
			totalfile.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")
