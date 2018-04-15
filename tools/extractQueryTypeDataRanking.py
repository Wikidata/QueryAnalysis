import argparse
import csv
import gzip
import os
import shutil
import sys
import urlparse

import pandas
from itertools import izip

import config
import fieldRanking

from utility import utility

# Generates a list of all query types sorted in descending order by number of
# appearance based on fieldRanking.tsv's output for the field #QueryType
# The file contains all metrics that are specific for the query type (ignoring
# things like subject and object names) as well as one example query for this
# query type
# TODO: Setup command line parameters

fieldBasedOnQueryType = ['#Valid', '#QuerySize', '#VariableCountHead',
                         '#VariableCountPattern', '#TripleCountWithService',
                         '#QueryComplexity', '#SubjectsAndObjects', '#Predicates',
                         '#Categories', '#UsedSparqlFeatures']

# Number of query types to be extracted, use 0 for infinity

parser = argparse.ArgumentParser(
    description="Generates a list of all query types sorted in descending"
    + " order by nomber of appearance based on fieldRanking.py's output for"
    + " the field #QueryType")
parser.add_argument("--monthsFolder", "-m",
                    default=config.monthsFolder, type=str,
                    help="The folder in which the months directories are"
                    + " residing.")
parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and execute"
                    + " anyways", action="store_true")
parser.add_argument("--topNumber", "-n", default=0, type=int,
                    help="The top n query types should be present in the "
                    + "generated file.")
parser.add_argument("month", type=str,
                    help="The month from which the query type file should be "
                    + "generated.")

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

if os.path.isfile(utility.addMissingSlash(args.monthsFolder) +
                  utility.addMissingSlash(args.month) + "locked") \
   and not args.ignoreLock:
    print "ERROR: The month " + args.month + " is being edited at the moment."
    + " Use -i if you want to force the execution of this script."
    sys.exit()

os.chdir(utility.addMissingSlash(args.monthsFolder)
         + utility.addMissingSlash(args.month))

pathBase = "queryTypeDataRanking/"
fileName = "Query_Type_Data_Ranking.tsv"

processedPrefix = config.processedPrefix
sourcePrefix = config.sourcePrefix

if not os.path.exists(pathBase):
    os.makedirs(pathBase)

with open(pathBase + fileName, "w") as types:
    typeWriter = csv.DictWriter(types, None, delimiter="\t")

    th = {"#QueryType":"#QueryType", "#QueryTypeCount":"#QueryTypeCount", "#ExampleQuery":"#ExampleQuery"}

    typeWriter.fieldnames = ["#QueryType", "#QueryTypeCount", "#ExampleQuery"]

    for h in fieldBasedOnQueryType:
        th[h] = h
        typeWriter.fieldnames.append(h)
    typeWriter.writerow(th)

    i = 0

    queryTypes = dict()

    queryTypeCount = fieldRanking.fieldRanking(args.month, "QueryType", monthsFolder = args.monthsFolder, ignoreLock = args.ignoreLock)
    for i, (k, v) in enumerate(sorted(queryTypeCount.iteritems(), key=lambda (k, v): (v, k), reverse=True)):
        if i >= args.topNumber and args.topNumber != 0:
            break
        i += 1

        queryTypes[k] = v

    for i in xrange(1, 32):
        if not (os.path.exists(processedPrefix + "%02d" % i + ".tsv.gz")
                and os.path.exists(sourcePrefix + "%02d" % i + ".tsv.gz")):
            continue
        print "Working on %02d" % i
        with gzip.open(processedPrefix + "%02d" % i + ".tsv.gz") as p, \
                gzip.open(sourcePrefix + "%02d" % i + ".tsv.gz") as s:
            pReader = csv.DictReader(p, delimiter="\t")
            sReader = csv.DictReader(s, delimiter="\t")

            for processed, source in izip(pReader, sReader):
                queryType = processed["#QueryType"]
                if queryType in queryTypes:

                    processedToWrite = dict()

                    d = dict(urlparse.parse_qsl(
                        urlparse.urlsplit(source['uri_query']).query))
                    if 'query' in d.keys():
                        processedToWrite['#ExampleQuery'] = d['query']
                    else:
                        processedToWrite['#ExampleQuery'] = ""
                        print "ERROR: Could not find query in uri_query:"
                        print source['uri_query']

                    for key in processed:
                        if key in fieldBasedOnQueryType:
                            processedToWrite[key] = processed[key]

                    processedToWrite["#QueryType"] = queryType
                    processedToWrite["#QueryTypeCount"] = queryTypes[queryType]

                    typeWriter.writerow(processedToWrite)
                    del queryTypes[queryType]

if len(queryTypes) > 0:
    print "Could not find examples for the following query types:"
    for key in queryTypes:
        print "\t" + key

df = pandas.read_csv(pathBase + fileName, sep="\t",
                     header=0, index_col=0)
df = df.sort_values(by=["#QueryTypeCount"], ascending=False)
df.to_csv(pathBase + fileName, sep="\t")

print "Done."
