# -*- coding: utf-8 -*-

import csv
import glob
import gzip
import os
import urlparse

import sys
from itertools import izip

processedPrefix = "QueryProcessedOpenRDF"
processedSuffix = ".tsv.gz"
sourcePrefix = "queryCnt"


# iterates over all processed files in the given folder
def processMonth(handler, month, monthsFolder="/a/akrausetud/months"):
	for filename in glob.glob(monthsFolder + "/processedLogData/" + processedPrefix + "*" + processedSuffix):
		day = os.path.basename(filename)[len(processedPrefix):][:-len(processedSuffix)]
		processDay(handler, int(day), month=month, monthsFolder=monthsFolder)


''' monthsFolder is only needed for development
'''


def processDay(handler, day, month, startIdx=0, endIdx=sys.maxint, monthsFolder="/a/akrausetud/months"):
	processedFileName = monthsFolder + "/" + month + "/processedLogData/" + processedPrefix + "%02d" % day + processedSuffix

	print "Working on: " + processedFileName
	with gzip.open(processedFileName) as p, open(
															monthsFolder + "/" + month + "/rawLogData/" + sourcePrefix + "%02d" % day + ".tsv") as s:
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
				processed['#hour'] = source['hour']
				processed['#day'] = day
				handler.handle(sparqlQuery, processed)
			elif i > endIdx:
				break
			i += 1