import csv
import os
import urlparse

from shutil import copyfile
from itertools import izip

fieldBasedOnQueryType = ['#QueryType', '#Valid', '#QuerySize', '#VariableCountHead', '#VariableCountPattern', '#TripleCountWithService', '#TripleCountNoService',
                         '#PIDs', '#Add', '#And', '#ArbitraryLengthPath', '#Avg', '#BindingSetAssignment', '#BNodeGenerato', '#Bound',
                         '#Clear', '#Coalesce', '#Compare', '#CompareAll', '#CompareAny', '#Copy', '#Count', '#Create', '#Datatype', '#DeleteData',
                         '#DescripbeOperator', '#Difference', '#Distinct', '#EmptySet', '#Exists', '#Extension', '#ExtensionElem', '#Filter',
                         '#FunctionCall', '#Group', '#GroupConcat', '#GroupElem', '#If', '#In', '#InsertData', '#Intersection', '#IRIFunction',
                         '#IsBNode', '#IsLiteral', '#Isnumeric', '#IsResource', '#IsURI', '#Join', '#Label', '#Lang', '#LangMatches', '#LeftJoin',
                         '#Like', '#ListMemberOperator', '#Load', '#LocalName', '#MathExpr', '#Max', '#Min', '#Modify', '#Move', '#MultiProjection',
                         '#Namespace', '#Not', '#Or', '#Order', '#OrderElem', '#Projection', '#ProjectionElem', '#ProjectionElemList', '#QueryRoot',
                         '#Reduced', '#Regex', '#SameTerm', '#Sample', '#Service', '#SingletonSet', '#Slice', '#StatementPattern', '#Str', '#Sum',
                         '#Union', '#ValueConstant', '#Var', '#ZeroLengthPath']

# Number of query types to be extracted, use 0 for infinity

TopN = 0
topNQueryTypes = []

writeExampleQueryToProcessed = True

pathBase = "QueryType"

subfolder = "queryTypeData/"

processedPrefix = "QueryProcessedOpenRDF"
sourcePrefix = "queryCnt"

if not os.path.exists(subfolder):
    os.makedirs(subfolder)
    
with open(pathBase + "/TotalQueryType_Ranking.tsv") as rankingFile:
    rankingReader = csv.DictReader(rankingFile, delimiter = "\t")
    for line in rankingReader:
        if len(topNQueryTypes) >= TopN and TopN != 0:
            break
        topNQueryTypes.append(line["QueryType"])

for i in xrange(1, 2):
    print "Working on %02d" % i
    with open(processedPrefix + "%02d" % i + ".tsv") as p, open(sourcePrefix + "%02d" % i + ".tsv") as s, open(
                                    subfolder + processedPrefix + "%02d" % i + ".tsv", "w") as user_p, open(
                                subfolder + sourcePrefix + "%02d" % i + ".tsv", "w") as user_s:
        pReader = csv.DictReader(p, delimiter="\t")
        sReader = csv.DictReader(s, delimiter="\t")

        pWriter = csv.DictWriter(user_p, None, delimiter="\t")
        sWriter = csv.DictWriter(user_s, None, delimiter="\t")

        for processed, source in izip(pReader, sReader):
            
            if pWriter.fieldnames is None:
                
                ph = dict()
                if writeExampleQueryToProcessed:
                    ph['#example_query'] = '#example_query'
                    pWriter.fieldnames = ['#example_query']
                for h in fieldBasedOnQueryType:
                    ph[h] = h
                    pWriter.fieldnames.append(h)
                pWriter.writerow(ph)

            if sWriter.fieldnames is None:
                sh = dict((h, h) for h in sReader.fieldnames)
                sWriter.fieldnames = sReader.fieldnames
                sWriter.writerow(sh)

            queryType = processed["#QueryType"]
            if topNQueryTypes.__contains__(queryType):
                    
                processedToWrite = dict()
                
                d = dict(urlparse.parse_qsl(urlparse.urlsplit(source['uri_query'].lower()).query))
                
                if writeExampleQueryToProcessed:
                    if 'query' in d.keys():
                        processedToWrite['#example_query'] = d['query']
                    else:
                        processedToWrite['#example_query'] = ""
                        print "ERROR: Could not find query in uri_query:"
                        print source['uri_query']
                
                for key in processed:
                    if key in fieldBasedOnQueryType and key !=  None:
                        processedToWrite[key] = processed[key]
                
                pWriter.writerow(processedToWrite)
                sWriter.writerow(source)
                topNQueryTypes.remove(queryType)
        if len(topNQueryTypes) <= 0:
                if TopN != 0:
                    print "Found entries for all top " + str(TopN) + " query types."
                else:
                    print "Found entries for all query types." 
                break

if len(topNQueryTypes) > 0:
    print "Could not find entries for the following query types:"
    for queryType in topNQueryTypes:
        print queryType
