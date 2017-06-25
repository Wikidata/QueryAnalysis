import argparse
from collections import defaultdict
from pprint import pprint

import sys

import processdata as processdata

parser = argparse.ArgumentParser(description="Counts the used tools/bots in the given folder")
parser.add_argument("processedLogDataFolder", type=str, help="the folder in which the processed log files are in")
parser.add_argument("rawLogDataFolder", type=str, help="the folder in which the raw log files are in")

if (len(sys.argv[1:]) == 0):
	parser.print_help()
	parser.exit()

args = parser.parse_args()

class CountToolsHandler:
	toolCounter = defaultdict(int)

	def handle(self, sparqlQuery, processed):
		self.toolCounter[processed['#ToolName']] += 1

	def printStatistic(self):
		pprint(sorted(self.toolCounter.iteritems(), key=lambda x: x[1], reverse=True))

handler = CountToolsHandler()

processdata.processFolder(handler, processedLogDataFolder=args.processedLogDataFolder,
                          rawLogDataFolder=args.rawLogDataFolder)

handler.printStatistic()
