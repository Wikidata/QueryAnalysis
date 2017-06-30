import csv
import os
from collections import defaultdict

from itertools import izip


# This script generates a table that maps how often each predicate is used in combination with each predicate
# TODO: Command line parameters

def writeOut(fieldValues, file, dictionary):
	header = "hour/combinedWith"
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


processedPrefix = "QueryProcessedOpenRDF"
sourcePrefix = "queryCnt"

pathBase = "PIDs"
pathSuffix = "Combinations"

if not os.path.exists(pathBase + pathSuffix):
	os.makedirs(pathBase + pathSuffix)

monthlyFieldValues = set()

monthlyData = dict()

for i in xrange(1, 2):
	print "Working on: %02d" % i
	with open(processedPrefix + "%02d" % i + ".tsv") as p, open(sourcePrefix + "%02d" % i + ".tsv") as s:

		dailyFieldValues = set()

		dailyData = dict()

		pReader = csv.DictReader(p, delimiter="\t")
		sReader = csv.DictReader(s, delimiter="\t")
		for processed, source in izip(pReader, sReader):
			if int(processed['#Valid']) != 1:
				continue

			keys_string = processed['#' + pathBase]

			keys_array = keys_string.split(",")

			for key in keys_array:
				if key not in dailyFieldValues:
					dailyFieldValues.add(key)
					dailyData[key] = defaultdict(int)
				if key not in monthlyFieldValues:
					monthlyFieldValues.add(key)
					monthlyData[key] = defaultdict(int)
				for match in keys_array:
					if match == key:
						continue
					dailyData[key][match] += 1
					monthlyData[key][match] += 1

	with open(pathBase + pathSuffix + "/" + "Day" + "%02d" % i + pathBase + ".tsv", "w") as dailyfile:
		writeOut(dailyFieldValues, dailyfile, dailyData)

with open(pathBase + pathSuffix + "/" + "Total" + pathBase + ".tsv", "w") as monthlyfile:
	writeOut(monthlyFieldValues, monthlyfile, monthlyData)
