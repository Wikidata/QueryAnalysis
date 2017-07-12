#!/usr/bin/python
# -*- coding: utf-8 -*-
import argparse
import os
import sys
from tabulate import tabulate

from postprocess import processdata
from utility import utility

parser = argparse.ArgumentParser(description="Tool to view the content of the processed query logs")
parser.add_argument("--monthsFolder", "-m", default="/a/akrausetud/months", type=str,
				help="The folder in which the months directory are residing.")
parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and execute anyways", action="store_true")
parser.add_argument("--startline", "-s", default=0, type=int, help="The line from which we want to start displaying the data.")
parser.add_argument("--endline", "-e", default=sys.maxint, type=int, help="The line where we want to stop displaying the data.")
parser.add_argument("month", type=str, help="The month from which lines should be displayed.")
parser.add_argument("day", type=int, help="The day of the month from which lines should be displayed.")
parser.add_argument("metrics", type=str, help="The metrics that should be show, separated by comma (e.g QuerySize,QueryType)")

if (len(sys.argv[1:]) == 0):
	parser.print_help()
	parser.exit()

args = parser.parse_args()

if os.path.isfile(utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(args.month) + "locked") and not args.ignoreLock:
	print "ERROR: The month " + args.month + " is being edited at the moment. Use -i if you want to force the execution of this script."
	sys.exit()

metrics = list()

for metric in args.metrics.split(","):
    metrics.append(utility.addMissingDoubleCross(metric))

class ViewDataHandler:

    def handle(self, sparqlQuery, processed):
        data = [[]]
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

processdata.processDay(handler, args.day, args.month, args.monthsFolder, startIdx=args.startline, endIdx=args.endline)
