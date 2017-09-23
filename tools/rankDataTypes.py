import argparse
import os
import re
import sys

from collections import defaultdict

import config

from postprocess import processdata
from utility import utility

parser = argparse.ArgumentParser(
    description="Tool to rank the used data types")
parser.add_argument("--monthsFolder", "-m", default=config.monthsFolder,
                    type=str, help="The folder in which the months directory"
                    + " are residing.")
parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and "
                    + "execute anyways", action="store_true")
parser.add_argument("month", type=str, help="The month from which lines "
                    + "should be displayed.")
parser.add_argument("--anonymous", "-a", action="store_true", help="Check to switch to ranking the anonymous data.")

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

if os.path.isfile(utility.addMissingSlash(args.monthsFolder)
                  + utility.addMissingSlash(args.month) + "locked") \
   and not args.ignoreLock:
    print "ERROR: The month " + args.month + " is being edited at the "
    + "moment. Use -i if you want to force the execution of this script."
    sys.exit()

ranking = defaultdict(int)

class rankDataTypesHandler:

    def handle(self, sparqlQuery, processed):
        for entry in re.findall(r'\^\^(.*?)( |\)|\\n)', str(sparqlQuery)):
            ranking[entry[0]] += 1

handler = rankDataTypesHandler()

if args.anonymous:
    processdata.processMonth(handler, args.month, args.monthsFolder, anonymous = True)
else:
    processdata.processMonth(handler, args.month, args.monthsFolder)

print "count\tdataType"
for k, v in sorted(ranking.iteritems(), key=lambda (k, v): (v, k), reverse=True):
    print str(v) + "\t" + k
