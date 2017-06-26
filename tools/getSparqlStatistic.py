import argparse
from collections import defaultdict

import sys

from postprocess import processdata

parser = argparse.ArgumentParser(description="Prints out the SPARQL statistic")
parser.add_argument("--monthsFolder", "-m", type=str, help="the folder in which the months directory are residing")
parser.add_argument("month", type=str, help="the month which we're interested in")

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
		if (processed['#Valid'] == 'VALID'):
			self.totalCount += 1
			for featureName in processed:
				if featureName in self.notStatisticNames:
					continue
				if processed[featureName] is not "0":
					self.statistic[featureName] += 1

	def __str__(self):
		result = ""
		for featureName, featureCount in sorted(self.statistic.iteritems()):
			result += '{:<28} {:>8}/{:<8} {:>5}%'.format(featureName, featureCount, self.totalCount,
			                                             round(float(featureCount) / self.totalCount * 100, 2)) + "\n"
		return result


handler = SparqlStatisticHandler()

processdata.processMonth(handler, args.month, monthsFolder=args.monthsFolder)

print handler
