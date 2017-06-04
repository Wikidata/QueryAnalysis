# -*- coding: utf-8 -*-

import csv
import getopt
import sys

from tabulate import tabulate
from itertools import izip

metrics = ["#QuerySize", "#TripleCountWithService"]

help = "Usage: viewQueryTypes.py -s <startline> -e <endline> -q <queryType>\nIf a query type was given, start- and endline are ignored.\nIf start- or endline are omitted, they are assumed to mean 2 (ignoring the header) and infinity respectively."
start = 1
end = 0
queryType = None

foundQueryType = True

foundStartLine = True

filename = "QueryTypesWithData.tsv"

try:
    opts, args = getopt.getopt(sys.argv[1:], "hs:e:q:", ["startline=", "endline=", "queryType="])
except getopt.GetoptError:
    print help
    sys.exit(2)
for opt, arg in opts:
    if opt == "-h":
        print help
        sys.exit()
    elif opt in ("-s", "--startline"):
        start = int(arg)
        foundStartLine = False
    elif opt in ("-e", "--endline"):
        end = int(arg)
    elif opt in ("-q", "--queryType"):
        queryType = arg
        foundQueryType = False

if end != 0 and start > end:
    print "ERROR: startline is greater than endline."
    sys.exit()
    
if queryType != None and (not foundStartLine or end != 0):
    print "WARNING: Ignoring start- or endline because query type was set."
    

def writeRow(line):
    data = [[]]
    for metric in metrics:
        data[0].append(line[metric]) 
    print "\nQueryType: " + line["QueryType"] + "\n"
    print tabulate(data, headers=metrics)
    print line["#example_query"] + "\n"

i = 1
with open(filename) as file:
    fileReader = csv.DictReader(file, delimiter="\t")

    for line in fileReader:
        i += 1
        if queryType != None:
            if line["QueryType"] == queryType:
                writeRow(line)
                foundQueryType = True
                break
        else:
            if i >= start and (i <= end or end == 0):
                foundStartLine = True
                writeRow(line)


if not foundQueryType:
    print "Could not find query type " + queryType
if not foundStartLine:
    print "Could not start displaying at line " + str(start) + " because the file only has " + str(i) + " lines."