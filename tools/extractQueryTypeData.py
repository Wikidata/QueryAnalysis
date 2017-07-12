import argparse
import csv
import gzip
import os
import sys
import urlparse

import pandas
from itertools import izip

from utility import utility

# Generates a list of all query types sorted in descending order by number of appearance based on fieldRanking.tsv's output for the field #QueryType
# The file contains all metrics that are specific for the query type (ignoring things like subject and object names) as well as one example query for this query type
# TODO: Setup command line parameters  

fieldBasedOnQueryType = ['#Valid', '#QuerySize', '#VariableCountHead', '#VariableCountPattern',
                         '#TripleCountWithService', '#TripleCountNoService',
                         '#PIDs', '#Add', '#And', '#ArbitraryLengthPath', '#Avg', '#BindingSetAssignment',
                         '#BNodeGenerato', '#Bound',
                         '#Clear', '#Coalesce', '#Compare', '#CompareAll', '#CompareAny', '#Copy', '#Count', '#Create',
                         '#Datatype', '#DeleteData',
                         '#DescripbeOperator', '#Difference', '#Distinct', '#EmptySet', '#Exists', '#Extension',
                         '#ExtensionElem', '#Filter',
                         '#FunctionCall', '#Group', '#GroupConcat', '#GroupElem', '#If', '#In', '#InsertData',
                         '#Intersection', '#IRIFunction',
                         '#IsBNode', '#IsLiteral', '#Isnumeric', '#IsResource', '#IsURI', '#Join', '#Label', '#Lang',
                         '#LangMatches', '#LeftJoin',
                         '#Like', '#ListMemberOperator', '#Load', '#LocalName', '#MathExpr', '#Max', '#Min', '#Modify',
                         '#Move', '#MultiProjection',
                         '#Namespace', '#Not', '#Or', '#Order', '#OrderElem', '#Projection', '#ProjectionElem',
                         '#ProjectionElemList', '#QueryRoot',
                         '#Reduced', '#Regex', '#SameTerm', '#Sample', '#Service', '#SingletonSet', '#Slice',
                         '#StatementPattern', '#Str', '#Sum',
                         '#Union', '#ValueConstant', '#Var', '#ZeroLengthPath']

# Number of query types to be extracted, use 0 for infinity

parser = argparse.ArgumentParser(
    description="Generates a list of all query types sorted in descending order by nomber of appearance based on fieldRanking.py's output for the field #QueryType")
parser.add_argument("--monthsFolder", "-m", default="/a/akrausetud/months", type=str,
                    help="The folder in which the months directories are residing.")
parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and execute anyways", action="store_true")
parser.add_argument("--topNumber", "-n", default=0, type=int, help="The top n query types should be present in the generated file.")
parser.add_argument("month", type=str, help="The month from which the query type file should be generated.")

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

if os.path.isfile(utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(args.month) + "locked") and not args.ignoreLock:
    print "ERROR: The month " + args.month + " is being edited at the moment. Use -i if you want to force the execution of this script."
    sys.exit()

os.chdir(utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(args.month))

pathBase = "queryTypeData/"

processedPrefix = "processedLogData/QueryProcessedOpenRDF"
sourcePrefix = "rawLogData/queryCnt"

if not os.path.exists(pathBase):
    os.makedirs(pathBase)

with open("QueryType/ranking/Full_Month_QueryType_Ranking.tsv") as rankingFile, open(pathBase + "Query_Type_Data.tsv", "w") as types:
    rankingReader = csv.DictReader(rankingFile, delimiter="\t")
    typeWriter = csv.DictWriter(types, None, delimiter="\t")

    th = dict()

    for h in rankingReader.fieldnames:
        th[h] = h

    typeWriter.fieldnames = list(rankingReader.fieldnames)

    th['#example_query'] = '#example_query'
    typeWriter.fieldnames.append('#example_query')
    
    for h in fieldBasedOnQueryType:
        th[h] = h
        typeWriter.fieldnames.append(h)
    typeWriter.writerow(th)

    i = 0

    queryTypes = dict()

    for line in rankingReader:
        if i >= args.topNumber and args.topNumber != 0:
            break
        i += 1

        queryTypes[line["QueryType"]] = line

    for i in xrange(1, 32):
        if not (os.path.exists(processedPrefix + "%02d" % i + ".tsv.gz") and os.path.exists(sourcePrefix + "%02d" % i + ".tsv")):
            continue
        print "Working on %02d" % i
        with gzip.open(processedPrefix + "%02d" % i + ".tsv.gz") as p, open(sourcePrefix + "%02d" % i + ".tsv") as s:
            pReader = csv.DictReader(p, delimiter="\t")
            sReader = csv.DictReader(s, delimiter="\t")

            for processed, source in izip(pReader, sReader):
                queryType = processed["#QueryType"]
                if queryType in queryTypes:

                    processedToWrite = dict()

                    d = dict(urlparse.parse_qsl(urlparse.urlsplit(source['uri_query']).query))
                    if 'query' in d.keys():
                        processedToWrite['#example_query'] = d['query']
                    else:
                        processedToWrite['#example_query'] = ""
                        print "ERROR: Could not find query in uri_query:"
                        print source['uri_query']

                    for key in processed:
                        if key in fieldBasedOnQueryType:
                            processedToWrite[key] = processed[key]

                    for key in queryTypes[queryType]:
                        processedToWrite[key] = queryTypes[queryType][key]

                    typeWriter.writerow(processedToWrite)
                    del queryTypes[queryType]

if len(queryTypes) > 0:
	print "Could not find examples for the following query types:"
	for key in queryTypes:
		print "\t" + key

df = pandas.read_csv(pathBase + "Query_Type_Data.tsv", sep="\t", header=0, index_col=0)
df = df.sort(["QueryType_count"], ascending=False)
df.to_csv(pathBase + "Query_Type_Data.tsv", sep="\t")

print "Done."
