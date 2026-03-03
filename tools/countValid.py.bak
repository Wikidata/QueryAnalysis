import argparse
import os
import sys
from collections import defaultdict
from postprocess import processdata
from utility import utility
import pprint
import config

parser = argparse.ArgumentParser(description="Counts the valid queries")
parser.add_argument("--monthsFolder", "-m", default=config.monthsFolder,
                    type=str, help="the folder in which the months directory"
                    + " are residing")
parser.add_argument("--ignoreLock", "-i",
                    help="Ignore locked file and execute anyways",
                    action="store_true")
parser.add_argument("month", type=str,
                    help="the month which we're interested in")

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

if os.path.isfile(utility.addMissingSlash(args.monthsFolder)
                  + utility.addMissingSlash(args.month) + "locked") \
   and not args.ignoreLock:
    print("ERROR: The month " + args.month + " is being edited at the moment."
          + " Use -i if you want to force the execution of this script.")
    sys.exit()


class CountValidityHandler:
    validCounter = defaultdict(int)

    def handle(self, sparqlQuery, processed):
        self.validCounter[processed['#Valid']] += 1

    def __str__(self):
        return pprint.pformat(self.validCounter)


handler = CountValidityHandler()

processdata.processMonth(handler, args.month, args.monthsFolder)

print(handler)
