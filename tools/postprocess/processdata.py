# -*- coding: utf-8 -*-
import csv
import glob
import gzip
import os
import urllib
import urlparse
from pprint import pprint
import sys
from itertools import izip

from utility import utility

processedFolder = "processedLogData/"
processedPrefix = "QueryProcessedOpenRDF"
processedSuffix = ".tsv.gz"
sourcePrefix = "QueryCnt"

anonymousDataFolder = "anonymousRawData/"
anonymousFilePrefix = "AnonymousQueryCnt"
anonymousFileSuffix = ".tsv.gz"

rankedQueryTypeFolder = "queryTypeDataRanking/"
rankedQueryTypeFile = "Query_Type_Data_Ranking.tsv"


def processMonth(handler, month, monthsFolder, anonymous = False, notifications = True):
    folderToSearch = processedFolder
    prefixToSearch = processedPrefix
    suffixToSearch = processedSuffix

    if anonymous:
        folderToSearch = anonymousDataFolder
        prefixToSearch = anonymousFilePrefix
        suffixToSearch = anonymousFileSuffix

    for filename in glob.glob(utility.addMissingSlash(monthsFolder)
                              + utility.addMissingSlash(month)
                              + folderToSearch + prefixToSearch + "*"
                              + suffixToSearch):
        day = os.path.basename(filename)[len(
            prefixToSearch):][:-len(suffixToSearch)]
        if anonymous:
            processDayAnonymous(handler, int(day), month, monthsFolder, notifications = notifications)
        else:
            processDay(handler, int(day), month, monthsFolder, notifications = notifications)


def processDay(handler, day, month, monthsFolder,
               startIdx=0, endIdx=sys.maxint, notifications = True):
    processedFileName = utility.addMissingSlash(monthsFolder) \
        + utility.addMissingSlash(month) \
        + processedFolder + processedPrefix + "%02d" % day \
        + processedSuffix

    if notifications:
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
                    source['uri_query']).query.replace(';', "%3B")))

                if 'query' in requestParameters.keys():
                    sparqlQuery = requestParameters['query']
                else:
                    sparqlQuery = None

                processed['#hour'] = source['hour']
                processed['#day'] = day
                processed['#user_agent'] = source['user_agent']
                processed['#http_status'] = source['http_status']
                processed['#ts'] = source['ts']
                handler.handle(sparqlQuery, processed)
            elif i > endIdx:
                break
            i += 1

def processDayAnonymous(handler, day, month, monthsFolder, startIdx=0, endIdx=sys.maxint, notifications = True):
    anonymousFileName = utility.addMissingSlash(monthsFolder) \
    + utility.addMissingSlash(month) \
    + anonymousDataFolder + anonymousFilePrefix + "%02d" % day + anonymousFileSuffix

    if notifications:
        print "Working on: " + anonymousFileName
    with gzip.open(anonymousFileName) as a:
        aReader = csv.DictReader(a, delimiter="\t")

        i = 0
        for anonymous in aReader:
            if startIdx <= i <= endIdx:
                sparqlQuery = urllib.unquote_plus(anonymous['#anonymizedQuery'])

                anonymous['#Valid'] = 'VALID'
                handler.handle(sparqlQuery, anonymous)
            elif i > endIdx:
                break
            i += 1

def processRankedQueryType(handler, month, monthsFolder, startIdx = 0, endIdx = sys.maxint, notifications = True):
    rankedQueryTypeFilename = utility.addMissingSlash(monthsFolder) + utility.addMissingSlash(month) + rankedQueryTypeFolder + rankedQueryTypeFile

    if notifications:
        print "Working on: " + rankedQueryTypeFilename

    with open(rankedQueryTypeFilename) as r:
        rReader = csv.DictReader(r, delimiter = "\t")

        i = 0
        for ranked in rReader:
            if startIdx <= i <= endIdx:
                handler.handle(ranked["#ExampleQuery"], ranked)
            elif i > endIdx:
                break
            i += 1
