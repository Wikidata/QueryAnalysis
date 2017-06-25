# -*- coding: utf-8 -*-

import csv
import glob
import os
import urlparse

import sys
from itertools import izip

processedPrefix = "QueryProcessedOpenRDF"
processedSuffix = ".tsv"
sourcePrefix = "queryCnt"


# iterates over all processed files in the given folder
def processFolder(folder, handler):
	for filename in glob.glob(folder + "/" + processedPrefix + "*" + processedSuffix):
		# parse day
		day = filename[len(processedPrefix):][:-len(processedSuffix)]
		processDay(day, handler, folder=folder)


def processDay(day, handler, startIdx=0, endIdx=sys.maxint, folder="."):
	os.chdir(folder)
	processedFileName = processedPrefix + "%02d" % day + processedSuffix
	print "Working on: " + processedFileName
	with open(processedFileName) as p, open(sourcePrefix + "%02d" % day + processedSuffix) as s:
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
