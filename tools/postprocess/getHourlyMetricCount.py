import argparse
from pprint import pprint

import sys

import processdata as processdata

parser = argparse.ArgumentParser(description="Counts for a given metric how often it occures per hour")
parser.add_argument("metric", type=str, help="the metric which we want to count")
parser.add_argument("processedLogDataFolder", type=str, help="the folder in which the processed log files are in")
parser.add_argument("rawLogDataFolder", type=str, help="the folder in which the raw log files are in")

if (len(sys.argv[1:]) == 0):
	parser.print_help()
	parser.exit()

args = parser.parse_args()


class HourlyMetricCountHandler:
	data = dict()

	def handle(self, sparqlQuery, processed):
		if (processed['#Valid'] == 'VALID'):
			if (processed['#hour'])
			pprint(processed)
		self.toolCounter[processed['#ToolName']] += 1

	def printStatistic(self):
		pprint(sorted(self.toolCounter.iteritems(), key=lambda x: x[1], reverse=True))


handler = HourlyMetricCountHandler()

processdata.processFolder(handler, processedLogDataFolder=args.processedLogDataFolder,
                          rawLogDataFolder=args.rawLogDataFolder)

handler.printStatistic()
