# -*- coding: utf-8 -*-

import csv
import glob
import gzip
import urlparse

import sys
from itertools import izip

processedPrefix = "QueryProcessedOpenRDF"
processedSuffix = ".tsv.gz"
sourcePrefix = "queryCnt"


# iterates over all processed files in the given folder
def processFolder(handler, rawLogDatafolder="/a/akrausetud/rawLogdata",
                  processedLogDataFolder="/a/akrausetud/processedLogDataForAllCategories"):
	for filename in glob.glob(processedLogDataFolder + "/" + processedPrefix + "*" + processedSuffix):
		day = filename[len(processedPrefix):][:-len(processedSuffix)]
		processDay(day, handler, rawLogDatafolder=rawLogDatafolder, processedLogDataFolder=processedLogDataFolder)


def processDay(day, handler, startIdx=0, endIdx=sys.maxint, rawLogDataFolder="/a/akrausetud/rawLogdata",
               processedLogDataFolder="/a/akrausetud/processedLogDataForAllCategories"):
	processedFileName = processedLogDataFolder + "/" + processedPrefix + "%02d" % day + processedSuffix

	print "Working on: " + processedFileName
	with gzip.open(processedFileName) as p, open(rawLogDataFolder + "/" + sourcePrefix + "%02d" % day + ".tsv") as s:
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

		# @todo add processFolderCummulatively which emulates a group by query
