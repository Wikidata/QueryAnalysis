import argparse
from collections import defaultdict

import sys

import processdata as processdata

parser = argparse.ArgumentParser(description="Prints out the SPARQL statistic")
parser.add_argument("processedLogDataFolder", type=str, help="the folder in which the processed log files are in")
parser.add_argument("rawLogDataFolder", type=str, help="the folder in which the raw log files are in")

if (len(sys.argv[1:]) == 0):
	parser.print_help()
	parser.exit()

args = parser.parse_args()


class SparqlStatisticHandler:
	notStatisticNames = ['#Valid', '#ToolName', '#ToolVersion', '#StringLengthWithComments', '#QuerySize',
	                     '#VariableCountHead',
	                     '#VariableCountPattern', '#TripleCountWithService', '#TripleCountNoService', '#QueryType',
	                     '#QIDs',
	                     '#original_line(filename_line)', '#ExampleQueryStringComparison',
	                     '#ExampleQueryParsedComparison', '#Categories', '#Predicates', '#SubjectsAndObjects']
	statistic = defaultdict(int)
	totalCount = 0

	def handle(self, sparqlQuery, processed):
		self.totalCount += 1
		for featureName in processed:
			if featureName in self.notStatisticNames:
				continue
			if processed[featureName] is not "0":
				self.statistic[featureName] += 1
			# int(line[featureName])

	def printStatistic(self):
		for featureName, featureCount in sorted(self.statistic.iteritems()):
			print('{:<28} {:>8}/{:<8} {:>5}%'.format(featureName, featureCount, self.totalCount,
			                                         round(float(featureCount) / self.totalCount * 100, 2)))


handler = SparqlStatisticHandler()

processdata.processFolder(handler, processedLogDataFolder=args.processedLogDataFolder,
                          rawLogDataFolder=args.rawLogDataFolder)

handler.printStatistic()
