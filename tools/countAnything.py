#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
from postprocess import processdata
import config
import os
import sys
from utility import utility
import operator

os.nice(19)

parser = argparse.ArgumentParser(
        description="Tool to sum up the uses of RDF properties in queries")
parser.add_argument("--monthsFolder", "-m", default=config.monthsFolder,
        type=str, help="The folder in which the months directory are residing.")
parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and "
        + "execute anyways", action="store_true")
parser.add_argument('--onlyValid', "-o", action='store_true', help="If set "
        + "only valid lines are being looked at")
parser.add_argument("month", type=str, help="The month from which lines "
        + "should be displayed.")
parser.add_argument("parameter", type=str, help="The parameter by which to group.")

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

if os.path.isfile(utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(args.month) + "locked") and not args.ignoreLock:
    print "ERROR: The month " + args.month + " is being edited at the moment. Use -i if you want to force the execution of this script."
    sys.exit()


class CountRdfPropertiesHandler:
	queryCount = 0
	propQueryCounts = {}
	
	def handle(self,sparqlQuery,processed):
		self.queryCount += 1

                if args.onlyValid:
                    if processed['#Valid'] is not 'VALID':
                        return

		props = processed[args.parameter].split(",")
		
		for prop in props:
			if prop in self.propQueryCounts:
				c = self.propQueryCounts[prop] + 1
			else:
				c = 1
			self.propQueryCounts[prop] = c
	
	def printResults(self):
		print "Queries: %d" % (self.queryCount)
		print "\n\n%s\tcount" % (args.parameter)
		for p, c in sorted(self.propQueryCounts.iteritems(), key=operator.itemgetter(1), reverse=True):
			print "%s\t%d" % (p,c)
	
handler = CountRdfPropertiesHandler()
processdata.processMonth(handler, args.month, args.monthsFolder)

handler.printResults()
