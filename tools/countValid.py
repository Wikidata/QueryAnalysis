import argparse
from collections import defaultdict

import sys

from postprocess import processdata

parser = argparse.ArgumentParser(description="Counts the valid queries")
parser.add_argument("processedLogDataFolder", type=str, help="the folder in which the processed log files are in")
parser.add_argument("rawLogDataFolder", type=str, help="the folder in which the raw log files are in")

if (len(sys.argv[1:]) == 0):
	parser.print_help()
	parser.exit()

args = parser.parse_args()


class CountValdHandler:
	validCounter = defaultdict(int)

	def handle(self, sparqlQuery, processed):
		# pprint(processed)
		self.validCounter[processed['#Valid']] += 1

	def printStatistic(self):
		print "Valid: \t\t" + str(self.validCounter['VALID']) + " " + str(
			float(self.validCounter['VALID']) / (self.validCounter['VALID'] + self.validCounter['INVALID']) * 100) + "%"
		print "Invalid:\t" + str(self.validCounter['INVALID']) + " " + str(float(self.validCounter['INVALID']) / (
			self.validCounter['VALID'] + self.validCounter['INVALID']) * 100) + "%"


handler = CountValdHandler()

processdata.processMonth(handler, processedLogDataFolder=args.processedLogDataFolder,
                         rawLogDataFolder=args.rawLogDataFolder)

handler.printStatistic()
