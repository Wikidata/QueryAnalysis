# -*- coding: utf-8 -*-

import csv
# import getopt
# import urllib
import urlparse

# import sys

# from tabulate import tabulate
# from itertools import izip

processedPrefix = "QueryProcessedOpenRDF"
sourcePrefix = "queryCnt"


def processDay(day, startIdx, endIdx, handler):
	with open(processedPrefix + "%02d" % day + ".tsv") as p, open(sourcePrefix + "%02d" % day + ".tsv") as s:
		pReader = csv.DictReader(p, delimiter="\t")
		sReader = csv.DictReader(s, delimiter="\t")

		i = 0
		for processed, source in izip(pReader, sReader):
			if startIdx <= i <= endIdx:
				requestParameters = dict(urlparse.parse_qsl(urlparse.urlsplit(source['uri_query']).query))
				if 'query' in requestParameters.keys():
					sparqlQuery = requestParameters['query']
				else:
					sparqlQuery = None

				handler.handle(sparqlQuery, processed)
			elif i > endIdx:
				break
			i += 1
