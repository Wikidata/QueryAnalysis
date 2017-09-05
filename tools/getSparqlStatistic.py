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
    statistic = defaultdict(int)
    totalCount = 0

    def handle(self, sparqlQuery, processed):
        if (processed['#Valid'] == 'VALID'):
            self.totalCount += 1
            usedSparqlFeatures = processed['#UsedSparqlFeatures']

            for usedSparqlFeature in usedSparqlFeatures.split(","):
                self.statistic[usedSparqlFeature.lstrip()] += 1

    def printKeys(self, keys):
        result = ""
        for featureName in keys:
            featureCount = self.statistic[featureName]
            result += '{:<28} {:>8}\t{:>5}%'.format(
                featureName, featureCount,
                round(float(featureCount) / self.totalCount * 100, 2)) + "\n"

        print(result)

    def printSparqlTranslation(self):
        self.statistic["Select"] = self.statistic["ProjectionElemList"]
        self.statistic["Order By"] = self.statistic["Order"]
        self.statistic["Group By"] = self.statistic["Group"]
        self.statistic["LimitAndOffset"] = self.statistic["Slice"]
        self.statistic["Minus"] = self.statistic["Difference"]
        self.statistic["Optional"] = self.statistic["LeftJoin"]
        self.statistic["Having*"] = self.statistic["Having"]

        # only print specified columns
        toPrintKeys = ["Select", "Distinct", "Limit", "Offset", "Order By",
                       "Filter", "And", "Union", "Opt", "Graph", "Not Exists",
                       "Minus", "Exists", "Count", "Max", "Min", "Avg", "Sum",
                       "Group By", "Having", "Service", "LangService"]

        self.printKeys(toPrintKeys)


handler = SparqlStatisticHandler()

processdata.processMonth(handler, args.month, args.monthsFolder)

handler.printSparqlTranslation()
