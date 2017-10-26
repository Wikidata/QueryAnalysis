import argparse
import os
import sys
from collections import defaultdict
from pprint import pprint

import config
from postprocess import processdata
from utility import utility
import itertools

parser = argparse.ArgumentParser(
    description=
    "Prints out the Operator statistic in a nicely formatted way which can be pasted directly into a Google Spreadsheet"
)
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


class OperatorStatisticHandler:
    statistic = defaultdict(int)
    totalCount = 0
    operators = [
        "Filter", "Join", "Union", "Optional", "Values", "Path", "Other"
    ]
    otherSparqlFeatures = set()

    def __init__(self):
        allOperatorsCombinations = set()

        # generate all possible combinations
        for i in [1, 2, 3, 4, 5, 6, 7]:
            for operator in itertools.combinations(self.operators, i):
                allOperatorsCombinations.add(operator)
        for operators in allOperatorsCombinations:
            self.statistic[", ".join(sorted(operators))] = 0

    def handle(self, sparqlQuery, processed):
        if (processed['#Valid'] == 'VALID'):
            self.totalCount += 1

            usedSparqlFeatures = set()

            for usedSparqlFeature in processed['#UsedSparqlFeatures'].split(
                    ","):
                usedSparqlFeature = usedSparqlFeature.lstrip()
                if usedSparqlFeature == "UnionGraphPattern":
                    usedSparqlFeature = "Union"
                elif usedSparqlFeature == "OptionalGraphPattern":
                    usedSparqlFeature = "Optional"
                elif usedSparqlFeature == "BindingValue":
                    usedSparqlFeature = "Values"
                elif usedSparqlFeature == "+" or usedSparqlFeature == "*":
                    usedSparqlFeature = "Path"
                elif usedSparqlFeature == "LangService":
                    usedSparqlFeature = ""
                elif usedSparqlFeature == "Filter" or usedSparqlFeature == "ExistsFunc" or usedSparqlFeature == "NotExistsFunc":
                    usedSparqlFeature = "Filter"
                elif usedSparqlFeature == "Join":
                    usedSparqlFeature = "Join"
                elif usedSparqlFeature == "SelectQuery" or usedSparqlFeature == "ConstructQuery" or usedSparqlFeature == "AskQuery" or usedSparqlFeature == "DescribeQuery" or usedSparqlFeature == "":
                    usedSparqlFeature = ""
                else:
                    self.otherSparqlFeatures.add(usedSparqlFeature)
                    usedSparqlFeature = "Other"

                usedSparqlFeatures.add(usedSparqlFeature)

            # check which operators are present:
            presentOperators = set()
            for operator in self.operators:
                if operator in usedSparqlFeatures:
                    presentOperators.add(operator)

            if len(presentOperators) == 0:
                self.statistic["None"] += 1
            else:
                self.statistic[", ".join(sorted(presentOperators))] += 1

    def printSparqlTranslation(self):
        result = ""
        i = 1
        for featureName, featureCount in sorted(self.statistic.iteritems()):
            print(featureName + "\t" + str(featureCount))
            #print(featureCount)
            i += 1

        print("")
        print(str(self.totalCount))

        pprint(self.otherSparqlFeatures)


handler = OperatorStatisticHandler()

processdata.processMonth(
    handler, args.month, args.monthsFolder, notifications=False)

print args.position

handler.printSparqlTranslation()
