#!/usr/bin/python
# -*- coding: utf-8 -*-
import argparse
import os
import sys
from tabulate import tabulate
from postprocess import processdata
from utility import utility
import config
from pprint import pprint

parser = argparse.ArgumentParser(
    description="Tool to view the content of the processed query logs")
parser.add_argument("--monthsFolder", "-m", default=config.monthsFolder,
                    type=str, help="The folder in which the months directory"
                    + " are residing.")
parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and "
                    + "execute anyways", action="store_true")
parser.add_argument('--onlyValid', "-o", action='store_true', help="If set "
                    + "only valid lines are being looked at")
parser.add_argument("--startline", "-s", default=0, type=int, help="The line"
                    + " from which we want to start displaying the data.")
parser.add_argument("--endline", "-e", default=sys.maxint, type=int,
                    help="The line where we want to stop displaying the data.")
parser.add_argument("--line", "-l", type=int, help="Set if you only want to display one specific line.")
parser.add_argument("month", type=str, help="The month from which lines "
                    + "should be displayed.")
parser.add_argument("day", type=int, help="The day of the month from which "
                    + "lines should be displayed.")
parser.add_argument("metrics", type=str, help="The metrics that should be "
                    + "show, separated by comma (e.g QuerySize,QueryType)")
parser.add_argument("--metricsNotNull", "-n", default="", type=str,
                    help="The list of metrics that shouldn't be null, "
                    + "separated by comma")
parser.add_argument("--anonymous", "-a", action="store_true", help="Check to switch to viewing the anonymous data."
                    + " WARNING: No processed metrics are available for anonymous data because the anonymous files"
                    + " do not synch up to the processed files due to dropping the invalid lines.")

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

startLine = args.startline
endLine = args.endline

if args.line != None:
    startLine = args.line
    endLine = args.line

if os.path.isfile(utility.addMissingSlash(args.monthsFolder)
                  + utility.addMissingSlash(args.month) + "locked") \
   and not args.ignoreLock:
    print "ERROR: The month " + args.month + " is being edited at the "
    + "moment. Use -i if you want to force the execution of this script."
    sys.exit()

metrics = list()
metricsNotNull = list()

for metric in args.metrics.split(","):
    metrics.append(utility.addMissingDoubleCross(metric))

if args.metricsNotNull is not "":
    for metric in args.metricsNotNull.split(","):
        metricsNotNull.append(utility.addMissingDoubleCross(metric))


class ViewDataHandler:

    def handle(self, sparqlQuery, processed):
        data = [[]]
        if args.onlyValid:
            if processed['#Valid'] is not 'VALID':
                return

        for metricNotNull in metricsNotNull:
            if processed[metricNotNull] is '' \
               or processed[metricNotNull] is "NONE" \
               or processed[metricNotNull] is 0:
                return

        for metric in metrics:
            data[0].append(processed[metric])
            print tabulate(data, headers=metrics)
            print "Query:"
            if sparqlQuery is None:
                print "Error: Could not find query in uri_query."
            else:
                print sparqlQuery
                print ""


handler = ViewDataHandler()

if args.anonymous:
    processdata.processDayAnonymous(handler, args.day, args.month, args.monthsFolder, startIdx=startLine, endIdx=endLine)
else:
    processdata.processDay(handler, args.day, args.month, args.monthsFolder, startIdx=startLine, endIdx=endLine)
