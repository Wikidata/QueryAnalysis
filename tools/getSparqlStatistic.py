import argparse
import os
import sys
from collections import defaultdict
from postprocess import processdata
from utility import utility
import config
from pprint import pprint

parser = argparse.ArgumentParser(description="Prints out the SPARQL statistic")
parser.add_argument("--monthsFolder", "-m", default=config.monthsFolder,
                    type=str, help="the folder in which the months directory "
                    + "are residing")
parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and execute"
                    + " anyways", action="store_true")
parser.add_argument("month", type=str,
                    help="the month which we're interested in")

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

if os.path.isfile(utility.addMissingSlash(args.monthsFolder)
                  + utility.addMissingSlash(args.month) + "locked") \
   and not args.ignoreLock:
    print "ERROR: The month " + args.month + " is being edited at the moment."
    + " Use -i if you want to force the execution of this script."
    sys.exit()


class SparqlStatisticHandler:
    notStatisticNames = ['#Valid', '#ToolName', '#ToolVersion',
                         '#StringLengthWithComments', '#QuerySize',
                         '#VariableCountHead', '#VariableCountPattern',
                         '#TripleCountWithService', '#TripleCountNoService',
                         '#QueryType', '#QIDs',
                         '#original_line(filename_line)',
                         '#ExampleQueryStringComparison',
                         '#ExampleQueryParsedComparison',
                         '#Categories', '#Predicates', '#SubjectsAndObjects']
    statistic = defaultdict(int)
    totalCount = 0

    def handle(self, sparqlQuery, processed):
        pprint(sparqlQuery)
        if (processed['#Valid'] == 'VALID' or processed['#Valid'] == '1'):
            self.totalCount += 1
            for featureName in processed:
                if featureName in self.notStatisticNames:
                    continue
                if processed[featureName] is not "0":
                    self.statistic[featureName] += 1

    def __str__(self):
        result = ""
        for featureName, featureCount in sorted(self.statistic.iteritems()):
            result += '{:<28} {:>8}/{:<8} {:>5}%'.format(
                featureName, featureCount, self.totalCount,
                round(float(featureCount) / self.totalCount * 100, 2)) + "\n"

        return result


handler = SparqlStatisticHandler()

processdata.processMonth(handler, args.month, args.monthsFolder)

print(handler)
