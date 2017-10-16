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

    def handle(self, sparqlQuery, processed):
        if (processed['#Valid'] == 'VALID'):
            self.totalCount += 1
            usedSparqlFeatures = set()
            for usedSparqlFeature in processed['#UsedSparqlFeatures'].split(
                    ","):
                if usedSparqlFeature == " UnionGraphPattern":
                    usedSparqlFeature = "Union"
                elif usedSparqlFeature == " OptionalGraphPattern":
                    usedSparqlFeature = "Optional"
                usedSparqlFeatures.add(usedSparqlFeature.lstrip())

            operators = ["Filter", "Join", "Union", "Optional"]

            allOperatorsCombinations = set()

            # generate all possible combinations
            for i in [1, 2, 3, 4]:
                for operator in itertools.combinations(operators, i):
                    allOperatorsCombinations.add(operator)

            noOperator = True

            for operatorCombination in allOperatorsCombinations:
                # check if this operator combination is present in current
                # query
                allOperatorsPresent = True
                for operator in operatorCombination:
                    if operator not in usedSparqlFeatures:
                        allOperatorsPresent = False

                if allOperatorsPresent:
                    self.statistic[", ".join(operatorCombination)] += 1
                    noOperator = False

            if noOperator:
                self.statistic["None"] += 1

    def printSparqlTranslation(self):
        result = ""
        i = 1
        for featureName, featureCount in sorted(self.statistic.iteritems()):
            #print(featureName)
            print(featureCount)
            i += 1

        print("")
        print(str(self.totalCount))


handler = OperatorStatisticHandler()

processdata.processMonth(
    handler, args.month, args.monthsFolder, notifications=False)

print args.position

handler.printSparqlTranslation()
