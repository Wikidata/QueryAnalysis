#!/usr/bin/python
# -*- coding: utf-8 -*-
import argparse

import sys
from tabulate import tabulate

from postprocess import processdata

parser = argparse.ArgumentParser(description="Tool to view the content of the processed query logs")
parser.add_argument("--monthsFolder", "-m", default="/a/akrausetud/month", type=str,
                    help="the folder in which the months directory are residing")
parser.add_argument("month", type=str, help="the month which we're interested in")
parser.add_argument("day", type=int, help="the day which we're interested in")
parser.add_argument("startline", type=int, help="the starting line of the file we're interested in")
parser.add_argument("endline", type=int, help="the ending line of the file we're interested in")

if (len(sys.argv[1:]) == 0):
	parser.print_help()
	parser.exit()

args = parser.parse_args()


class ViewDataHandler:
	metrics = ["#QuerySize", "#TripleCountWithService"]

	def handle(self, sparqlQuery, processed):
		data = [[]]
		for metric in self.metrics:
			data[0].append(processed[metric])
		print tabulate(data, headers=self.metrics)
		if sparqlQuery is None:
			print "Error: Could not find query in uri_query."
		else:
			print sparqlQuery


handler = ViewDataHandler()

processdata.processDay(handler, args.day, args.month, startIdx=args.startline, endIdx=args.endline,
                       monthsFolder=args.monthsFolder)
