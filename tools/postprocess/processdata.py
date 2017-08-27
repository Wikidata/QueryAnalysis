# -*- coding: utf-8 -*-
import csv
import glob
import gzip
import os
import urlparse
from pprint import pprint
import sys
from itertools import izip

from utility import utility

processedFolder = "processedLogData/"
processedPrefix = "QueryProcessedOpenRDF"
processedSuffix = ".tsv.gz"
sourcePrefix = "QueryCnt"


def processMonth(handler, month, monthsFolder):
    for filename in glob.glob(utility.addMissingSlash(monthsFolder)
                              + utility.addMissingSlash(month)
                              + processedFolder + processedPrefix + "*"
                              + processedSuffix):
        day = os.path.basename(filename)[len(
            processedPrefix):][:-len(processedSuffix)]
        processDay(handler, int(day), month, monthsFolder)


def processDay(handler, day, month, monthsFolder,
               startIdx=0, endIdx=sys.maxint):
    processedFileName = utility.addMissingSlash(monthsFolder) \
        + utility.addMissingSlash(month) \
        + processedFolder + processedPrefix + "%02d" % day \
        + processedSuffix

    print "Working on: " + processedFileName
    with gzip.open(processedFileName) as p, \
            gzip.open(utility.addMissingSlash(monthsFolder)
                      + utility.addMissingSlash(month) + "rawLogData/"
                      + sourcePrefix + "%02d" % day + ".tsv.gz") as s:
        pReader = csv.DictReader(p, delimiter="\t")
        sReader = csv.DictReader(s, delimiter="\t")

        i = 0
        for processed, source in izip(pReader, sReader):
            if startIdx <= i <= endIdx:
                requestParameters = dict(urlparse.parse_qsl(urlparse.urlsplit(
                    source['uri_query']).query))

                if 'query' in requestParameters.keys():
                    sparqlQuery = requestParameters['query']
                else:
                    sparqlQuery = None

                processed['#hour'] = source['hour']
                processed['#day'] = day
                processed['#user_agent'] = source['user_agent']
                handler.handle(sparqlQuery, processed)
            elif i > endIdx:
                break
            i += 1
