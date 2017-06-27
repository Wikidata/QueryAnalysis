import argparse
import os
from collections import defaultdict

import sys

from postprocess import processdata

parser = argparse.ArgumentParser(
	description="Counts for a given metric how often it occurs per hour. Creates then daily and a monthly tsv file containg the hour, the metric and the queryCount")
parser.add_argument("metric", type=str, help="the metric which we want to count (without #)")
parser.add_argument("--monthsFolder", "-m", default="/a/akrausetud/months", type=str,
                    help="the folder in which the months directory are residing")
parser.add_argument("month", type=str, help="the month which we're interested in")

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
		if (processed['#Valid'] == 'VALID' or processed['#Valid'] == '1'):
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
		if not os.path.exists(outputFolder + "/" + self.metric):
			os.makedirs(outputFolder + "/" + self.metric)

		header = "hour\t" + self.metric + "\tcount\n"
		for day, data in self.dailyData.iteritems():
			with open(outputFolder + self.metric + "/" + "%02d" % day
					          + "ClassifiedBotsData.tsv", "w") as outputFile:
				outputFile.write(header)
				for hour, metricDict in data.iteritems():
					for metric in metricDict.iterkeys():

						outputFile.write(str(hour) + "\t" + str(metric)
						                 + "\t" + str(data[hour][metric]) + "\n")

		with open(outputFolder + self.metric + "/" + "TotalClassifiedBotsData.tsv",
		          "w") as outputFile:
			outputFile.write(header)
			for hour, metricDict in self.monthlyData.iteritems():
				for metric in metricDict.iterkeys():
					outputFile.write(str(hour) + "\t" + str(metric) + "\t" + str(self.monthlyData[hour][metric]) + "\n")



handler = HourlyMetricCountHandler(args.metric)

processdata.processMonth(handler, args.month, args.monthsFolder)

handler.saveToFiles(args.monthsFolder + "/" + args.month + "/processedLogData/hourlyMetricCountData")
