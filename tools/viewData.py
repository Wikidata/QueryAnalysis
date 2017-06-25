# -*- coding: utf-8 -*-

import csv
import getopt
import urlparse

import sys
from itertools import izip
from tabulate import tabulate

metrics = ["#QuerySize", "#TripleCountWithService"]

help = 'Usage: viewData.py -d <day> -s <startline> -e <endline>'
day = 0
start = 0
end = 0
day_given = False
start_given = False
end_given = False

processedPrefix = "QueryProcessedOpenRDF"
sourcePrefix = "queryCnt"

try:
    opts, args = getopt.getopt(sys.argv[1:], "hd:s:e:", ["day=", "startline=", "endline="])
except getopt.GetoptError:
    print help
    sys.exit(2)
for opt, arg in opts:
    if opt == "-h":
        print help
        sys.exit()
    elif opt in ("-d", "--day"):
        day = int(arg)
        day_given = True
    elif opt in ("-s", "--startline"):
        start = int(arg)
        start_given = True
    elif opt in ("-e", "--endline"):
        end = int(arg)
        end_given = True

if not (day_given and start_given and end_given):
    print help
    sys.exit()

with open(processedPrefix + "%02d" % day + ".tsv") as p, open(sourcePrefix + "%02d" % day + ".tsv") as s:
    pReader = csv.DictReader(p, delimiter="\t")
    sReader = csv.DictReader(s, delimiter="\t")

    i = 0
    for processed, source in izip(pReader, sReader):
        if start <= i <= end:
            data = [[]]
            for metric in metrics:
                data[0].append(processed[metric])
            print tabulate(data, headers=metrics)
            d = dict(urlparse.parse_qsl(urlparse.urlsplit(source['uri_query']).query))
            if 'query' in d.keys():
                print d['query']
            else:
                print "Could not find query in uri_query."

        i += 1
