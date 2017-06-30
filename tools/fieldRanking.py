import argparse
import csv
import os
import sys

from collections import defaultdict
from itertools import izip

from postprocess import processdata
from utility import utility


# This script creates descending rankings for each day for all metrics (in the array metrics)
# TODO: Command line parameters

parser = argparse.ArgumentParser(
	description="This script creates descending rankings for each day for all metrics given.")
parser.add_argument("--monthsFolder", "-m", default="/a/akrausetud/months", type=str,
                    help="The folder in which the months directory are residing.")
parser.add_argument("--outputPath", "-o", type=str, help="The path where the output files should be generated.")
parser.add_argument("--nosplitting", "-n", help="Check if you do not want the script to split entries at commas and count each part separately\n" +
				"but instead just to sort such entries and count them as a whole.", action="store_true")
parser.add_argument("metric", type=str, help="The metric that should be ranked")
parser.add_argument("month", type=str, help="The month for which the ranking should be generated.")


if (len(sys.argv[1:]) == 0):
	parser.print_help()
	parser.exit()

args = parser.parse_args()

argMetric = args.metric
metric = ""

if argMetric.startswith("#"):
	metric = argMetric[1:]
else:
	metric = argMetric

pathBase = utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(args.month) + utility.addMissingSlash(metric) 

if args.outputPath is not None:
	pathBase = args.outputPath

if not os.path.exists(pathBase):
	os.makedirs(pathBase)

header = metric + "\t" + metric + "_count\tpercentage\n"

class FieldRankingHandler:
	totalCount = 0
	totalMetricCounts = defaultdict(int)
	
	dailyCount = defaultdict(int)
	dailyMetricCount = dict()
	
	def handle(self, sparqlQuery, processed):
		if processed['#Valid'] != "VALID":
			return
		field_array = str(processed["#" + metric]).split(",")
		if args.nosplitting:			
			field_array = sorted(field_array)
			self.countQuery(processed["#day"])
			self.countEntry(utility.listToString(field_array), processed["#day"])
		else:			
			self.countQuery(processed["#day"])
			for entry in field_array:
				self.countEntry(entry, processed["#day"])
				
	def countEntry(self, entry, day):
		if (day not in self.dailyMetricCount):
			self.dailyMetricCount[day] = defaultdict(int)
		self.totalMetricCounts[entry] += 1
		self.dailyMetricCount[day][entry] += 1
		
	def countQuery(self, day):
		if (day not in self.dailyMetricCount):
			self.dailyMetricCount[day] = defaultdict(int)
		self.totalCount += 1
		self.dailyCount[day] += 1
				
	def writeOut(self):
		self.writeCount(pathBase + "Full_Month_" + metric + "_Ranking.tsv", self.totalMetricCounts, self.totalCount)
		for day in self.dailyCount:
			self.writeCount(pathBase + "Day_" + str(day) + "_" + metric + "_Ranking.tsv", self.dailyMetricCount[day], self.dailyCount[day])
			
	
	def writeCount(self, filename, metricsCount, count):
		with open(filename, "w") as file:
			file.write(header)
			for k, v in sorted(metricsCount.iteritems(), key=lambda (k, v): (v, k), reverse=True):
				percentage = float(v) / count * 100
				file.write(str(k) + "\t" + str(v) + "\t" + str(percentage) + "\n")
				
handler = FieldRankingHandler()

processdata.processMonth(handler, args.month, args.monthsFolder)

handler.writeOut()