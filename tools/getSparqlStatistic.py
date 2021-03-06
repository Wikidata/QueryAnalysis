import argparse
import os
import sys
from collections import defaultdict
from pprint import pprint

import config
from postprocess import processdata
from utility import utility

parser = argparse.ArgumentParser(description="Prints out the SPARQL statistic")
parser.add_argument(
    "--monthsFolder",
    "-m",
    default=config.monthsFolder,
    type=str,
    help="the folder in which the months directory " + "are residing")
parser.add_argument(
    "--ignoreLock",
    "-i",
    help="Ignore locked file and execute" + " anyways",
    action="store_true")
parser.add_argument(
    "--position",
    "-p",
    default="default position",
    type=str,
    help="The position to be displayed before the data.")
parser.add_argument(
    "month", type=str, help="the month which we're interested in")

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

if os.path.isfile(utility.addMissingSlash(args.monthsFolder)
                  + utility.addMissingSlash(args.month) + "locked") \
   and not args.ignoreLock:
    print("ERROR: The month " + str(args.month) +
          " is being edited at the moment." +
          " Use -i if you want to force the execution of this script.")
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
        i = 1
        for featureName in keys:
            featureCount = self.statistic[featureName]
            # result += featureName + ": " + str(featureCount) + "\n"
            result += str(featureCount) + "\n"

            i += 1

        print(result)

    def printSparqlTranslation(self):
        self.statistic["Select"] = self.statistic["SelectQuery"]
        self.statistic["Ask"] = self.statistic["AskQuery"]
        self.statistic["Describe"] = self.statistic["DescribeQuery"]
        self.statistic["Construct"] = self.statistic["ConstructQuery"]
        self.statistic["Order By"] = self.statistic["OrderClause"]
        self.statistic["Union"] = self.statistic["UnionGraphPattern"]
        self.statistic["Optional"] = self.statistic["OptionalGraphPattern"]
        self.statistic["Not Exists"] = self.statistic["NotExistsFunc"]
        self.statistic["Minus"] = self.statistic["MinusGraphPattern"]
        self.statistic["Exists"] = self.statistic["ExistsFunc"]
        self.statistic["Group By"] = self.statistic["GroupClause"]
        self.statistic["Having"] = self.statistic["HavingClause"]
        self.statistic["Service"] = self.statistic["ServiceGraphPattern"]

        self.statistic["And"] = self.statistic["Join"]
        self.statistic["Values"] = self.statistic["BindingValue"]
        self.statistic["'+"] = self.statistic["+"]

        self.statistic["Subquery"] = self.statistic["SubSelect"]

        # only print specified columns
        toPrintKeys = [
            "Select", "Ask", "Describe", "Construct", "Distinct", "Limit",
            "Offset", "Order By", "Filter", "And", "Union", "Optional",
            "Graph", "Not Exists", "Minus", "Exists", "Count", "Max", "Min",
            "Avg", "Sum", "Group By", "Having", "Service", "LangService",
            "Sample", "Bind", "GroupConcat", "Reduced", "Values", "'+", "*",
            "Subquery"
        ]

        self.printKeys(toPrintKeys)
        print(" ")
        print(str(self.totalCount))


handler = SparqlStatisticHandler()

processdata.processMonth(
    handler, args.month, args.monthsFolder, notifications=False)

print args.position

handler.printSparqlTranslation()
