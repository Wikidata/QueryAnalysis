import argparse
import os
from collections import defaultdict

import sys

from postprocess import processdata

parser = argparse.ArgumentParser(
	description="Counts for a given metric how often it occurs per hour. Creates then daily and a monthly tsv file containg the hour, the metric and the queryCount")
parser.add_argument("metric", type=str, help="the metric which we want to count (without #)")
parser.add_argument("processedLogDataFolder", type=str, help="the folder in which the processed log files are in")
parser.add_argument("rawLogDataFolder", type=str, help="the folder in which the raw log files are in")
parser.add_argument("outputFolder", type=str, help="the folder in which the resulting tsv files should be put in")

if (len(sys.argv[1:]) == 0):
	parser.print_help()
	parser.exit()

args = parser.parse_args()


class HourlyMetricCountHandler:
	dailyData = dict()
	monthlyData = dict()
	metric = str

	def __init__(self, metric):
		self.metric = metric

	def handle(self, sparqlQuery, processed):
		if (processed['#Valid'] == 'VALID'):
			if (processed['#day'] not in self.dailyData):
				self.dailyData[processed['#day']] = dict()
			if (processed['#hour'] not in self.dailyData[processed['#day']]):
				self.dailyData[processed['#day']][processed['#hour']] = defaultdict(int)
			if (processed['#hour'] not in self.monthlyData):
				self.monthlyData[processed['#hour']] = defaultdict(int)

			self.dailyData[processed['#day']][processed['#hour']][processed['#' + self.metric]] += 1
			self.monthlyData[processed['#hour']][processed['#' + self.metric]] += 1

	def saveToFiles(self, outputFolder):
		outputFolder = outputFolder + "/"
		if not os.path.exists(outputFolder + "classifiedBotsData/" + self.metric):
			os.makedirs(outputFolder + "classifiedBotsData/" + self.metric)

		header = "hour\t" + self.metric + "\tcount\n"
		for day, data in self.dailyData.iteritems():
			with open(outputFolder + "classifiedBotsData/" + self.metric + "/" + "%02d" % day
					          + "ClassifiedBotsData.tsv", "w") as outputFile:
				outputFile.write(header)
				for hour, metricDict in data.iteritems():
					for metric in metricDict.iterkeys():

						outputFile.write(str(hour) + "\t" + str(metric)
						                 + "\t" + str(data[hour][metric]) + "\n")

		with open(outputFolder + "classifiedBotsData/" + self.metric + "/" + "TotalClassifiedBotsData.tsv",
		          "w") as outputFile:
			outputFile.write(header)
			for hour, metricDict in self.monthlyData.iteritems():
				for metric in metricDict.iterkeys():
					outputFile.write(str(hour) + "\t" + str(metric) + "\t" + str(self.monthlyData[hour][metric]) + "\n")



handler = HourlyMetricCountHandler(args.metric)

processdata.processMonth(handler, processedLogDataFolder=args.processedLogDataFolder,
                         rawLogDataFolder=args.rawLogDataFolder)

handler.saveToFiles(args.outputFolder)
