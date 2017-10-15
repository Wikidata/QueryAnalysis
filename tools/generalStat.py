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
    description="Prints out general statistics about FIRST/COPY")
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
parser.add_argument("--position", "-p", default="default position", type=str, help="The position to be displayed before the data.")
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


class GeneralStatisticsHandler:
    statistic = defaultdict(int)
    totalCount = 0

    def handle(self, sparqlQuery, processed):
        if (processed['#Valid'] == 'VALID'):
            self.totalCount += 1
            self.statistic[processed['#First']] += 1
            self.statistic[processed['#QueryComplexity']] += 1
            if processed['#ExampleQueryStringComparison'] != "NONE":
                self.statistic['EXAMPLE_STRING'] += 1
            if processed['#ExampleQueryParsedComparison'] != "NONE":
                self.statistic['EXAMPLE_PARSED'] += 1

    def printStat(self):
        pprint(self.statistic)
        print(
            "Month\tFirst\tCopy\tSIMPLE\tCOMPLEX\tEXAMPLE_STRING\tEXAMPLE_PARSED"
        )
        print(args.month + "\t" + str(self.statistic["FIRST"]) + "\t" + str(
            self.statistic["COPY"]) + "\t" + str(self.statistic["SIMPLE"]) +
              "\t" + str(self.statistic["COMPLEX"]) + "\t" +
              str(self.statistic["EXAMPLE_STRING"]) + "\t" +
              str(self.statistic["EXAMPLE_PARSED"]))


handler = GeneralStatisticsHandler()

processdata.processMonth(handler, args.month, args.monthsFolder, notifications = False)

print args.position
print ""

handler.printStat()
