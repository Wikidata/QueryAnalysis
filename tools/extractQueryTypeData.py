import csv
import os
import urlparse

import pandas
from itertools import izip

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

TopN = 0

writeExampleQueryToProcessed = True

pathBase = "QueryType"

subfolder = "queryTypeData"

outputFilePath = subfolder + "/QueryTypesWithData.tsv"

processedPrefix = "QueryProcessedOpenRDF"
sourcePrefix = "queryCnt"

if not os.path.exists(subfolder):
	os.makedirs(subfolder)

with open(pathBase + "/TotalQueryType_Ranking.tsv") as rankingFile, open(outputFilePath, "w") as types:
	rankingReader = csv.DictReader(rankingFile, delimiter="\t")
	typeWriter = csv.DictWriter(types, None, delimiter="\t")

	th = dict()

	for h in rankingReader.fieldnames:
		th[h] = h

	typeWriter.fieldnames = list(rankingReader.fieldnames)

	if writeExampleQueryToProcessed:
		th['#example_query'] = '#example_query'
		typeWriter.fieldnames.append('#example_query')
	for h in fieldBasedOnQueryType:
		th[h] = h
		typeWriter.fieldnames.append(h)
	typeWriter.writerow(th)

	i = 0

	queryTypes = dict()

	for line in rankingReader:
		if i >= TopN and TopN != 0:
			break
		i += 1

		queryTypes[line["QueryType"]] = line

	for i in xrange(1, 2):
		with open(processedPrefix + "%02d" % i + ".tsv") as p, open(sourcePrefix + "%02d" % i + ".tsv") as s:
			pReader = csv.DictReader(p, delimiter="\t")
			sReader = csv.DictReader(s, delimiter="\t")

			for processed, source in izip(pReader, sReader):
				queryType = processed["#QueryType"]
				if queryType in queryTypes:

					processedToWrite = dict()

					if writeExampleQueryToProcessed:
						d = dict(urlparse.parse_qsl(urlparse.urlsplit(source['uri_query'].lower()).query))
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

df = pandas.read_csv(outputFilePath, sep="\t", header=0, index_col=0)
df = df.sort(["QueryType_count"], ascending=False)
df.to_csv(outputFilePath, sep="\t")

print "Done."
