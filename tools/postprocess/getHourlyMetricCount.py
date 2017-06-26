import argparse
import os
from collections import defaultdict

import sys

import processdata as processdata

parser = argparse.ArgumentParser(description="Counts for a given metric how often it occures per hour")
parser.add_argument("metric", type=str, help="the metric which we want to count (without #)")
parser.add_argument("processedLogDataFolder", type=str, help="the folder in which the processed log files are in")
parser.add_argument("rawLogDataFolder", type=str, help="the folder in which the raw log files are in")

if (len(sys.argv[1:]) == 0):
	parser.print_help()
	parser.exit()

args = parser.parse_args()


class HourlyMetricCountHandler:
	data = dict()
	metric = str

	def __init__(self, metric):
		self.metric = metric

	def handle(self, sparqlQuery, processed):
		if (processed['#Valid'] == 'VALID'):
			if (processed['#day'] not in self.data):
				self.data[processed['#day']] = dict()
			if (processed['#hour'] not in self.data[processed['#day']]):
				self.data[processed['#day']][processed['#hour']] = defaultdict(int)
			self.data[processed['#day']][processed['#hour']][processed['#' + self.metric]] += 1

	def saveToFiles(self, outputFolder):
		if not os.path.exists(outputFolder + "classifiedBotsData/" + self.metric):
			os.makedirs(outputFolder + "classifiedBotsData/" + self.metric)
		for day, data in self.data.iteritems():
			header = "hour\t" + self.metric + "\tcount\n"
			with open(outputFolder + "classifiedBotsData/" + self.metric + "/" + "%02d" % day
					          + "ClassifiedBotsData.tsv", "w") as outputFile:
				outputFile.write(header)
				for hour, metricDict in data.iteritems():
					for metric in metricDict.iterkeys():
						outputFile.write(str(hour) + "\t" + str(metric)
						                 + "\t" + str(data[hour][metric]) + "\n")


handler = HourlyMetricCountHandler(args.metric)

processdata.processFolder(handler, processedLogDataFolder=args.processedLogDataFolder,
                          rawLogDataFolder=args.rawLogDataFolder)

handler.saveToFiles("/tmp/output/")
