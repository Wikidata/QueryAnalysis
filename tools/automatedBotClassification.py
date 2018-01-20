from __future__ import print_function

import argparse
import csv
import gzip
import os
import shutil
import subprocess
import sys
import urllib

import config
import fieldRanking

from postprocess import processdata
from utility import utility

parser = argparse.ArgumentParser(description = "This script searches for all combinations with occurences above the threshold.")
parser.add_argument("--monthsFolder", "-m", default=config.monthsFolder,
                    type=str, help="The folder in which the months directory "
                    + "are residing.")
parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and execute"
                    + " anyways", action="store_true")
parser.add_argument("--outputPath", "-o", type=str, help="The path where the "
                    + "output files should be generated.")
parser.add_argument("--logging", "-l", help="Enables file logging.",
                    action="store_true")
parser.add_argument("--filter", "-f", default="", type=str, help="Constraints "
                    + "used to limit the lines used to generate the output."
                    + " Default filter is Valid=^VALID$."
                    + " Enter as <metric>=<regex>,<othermetric>/<regex> (e.g."
                    + " QueryType=wikidataLastModified,ToolName=^USER$)"
                    + " NOTE: If you use this option you should probably also"
                    + " set the --outputPath to some value other than the "
                    + "default.")
parser.add_argument("month", type=str, help="The month for which the ranking should be generated.")
parser.add_argument("--threshold", "-t", default = 2000, type = int, help = "The threshold above which the cominations should be listed. Default is 2000.")

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

monthsFolder = utility.addMissingSlash(args.monthsFolder)
month = utility.addMissingSlash(args.month)

if os.path.isfile(monthsFolder + month + "locked") \
   and not ignoreLock:
    print ("ERROR: The month " + args.month + " is being edited at the moment."
    + " Use -i or ignoreLock = True if you want to force the execution of this script.")
    sys.exit()

subfolder = "automatedBotClassification/"

pathBase = monthsFolder + month + subfolder

if not os.path.exists(pathBase):
    os.makedirs(pathBase)

preBuildFolder = pathBase + "preBuildQueryTypeFiles/"

if not os.path.exists(preBuildFolder):
    os.makedirs(preBuildFolder)

tempForAnonymization = pathBase + "rawLogData/"

if not os.path.exists(tempForAnonymization):
    os.makedirs(tempForAnonymization)

manualCheckupFolder = pathBase + "manualCheckupFolder/"

if not os.path.exists(manualCheckupFolder):
    os.makedirs(manualCheckupFolder)

if args.outputPath is not None:
    pathBase = args.outputPath

filter = utility.filter()

filter.setup(args.filter)

queryType = "QueryType"
userAgent = "user_agent"

queryTypeOrder = list()

toolNamesToIgnore = list()

with open ("../userAgentClassification/toolNameForUserCategory.tsv") as toolNames:
    toolReader = csv.DictReader(toolNames, delimiter="\t")
    for entry in toolReader:
        toolNamesToIgnore.append(entry["tool names to be included in the user source category"])

def preparePath(path, directory, i):
    replacedDirectory = directory.replace("/", "SLASH")

    if len(replacedDirectory) > 140:
        replacedDirectory = replacedDirectory[:140] + str(i)
        i += 1

    fullPath = path + replacedDirectory + "/"

    if not os.path.exists(fullPath):
        os.makedirs(fullPath)

    return fullPath

class anonymizationReader():

    counter = 0

    def handle(self, sparqlQuery, processed):
        with open(preBuildFolder + queryTypeOrder[self.counter] + ".preBuildQueryType", "w") as queryTypeExample:
            print(sparqlQuery, file = queryTypeExample)
        self.counter += 1


class botClassification():
    queryTypes = dict()

    queryTypesCount = dict()

    def prepare(self):
        result = fieldRanking.fieldRanking(args.month, queryType, args.monthsFolder, ignoreLock = args.ignoreLock, filterParams = args.filter)
        for index, (keyOneEntry, keyOneEntryCount) in enumerate(sorted(result.iteritems(), key=lambda (k, v): (v, k), reverse = True)):
            if keyOneEntryCount < args.threshold:
                break
            self.queryTypes[keyOneEntry] = dict()
            self.queryTypesCount[keyOneEntry] = keyOneEntryCount

    def handle(self, sparqlQuery, processed):
        if not filter.checkLine(processed):
            return

        if processed["#ToolName"] in toolNamesToIgnore:
            return

        queryTypeEntry = processed["#" + queryType]
        if queryTypeEntry not in self.queryTypes:
            return

        queryTypeDict = self.queryTypes[queryTypeEntry]
        userAgentEntry = processed["#" + userAgent]
        if (userAgentEntry not in queryTypeDict):
            queryTypeDict[userAgentEntry] = list()
        queryTypeDict[userAgentEntry].append(sparqlQuery)

    def threshold(self):
        for queryTypeEntry, queryTypeDict in self.queryTypes.items():
            self.queryTypesCount[queryTypeEntry] = 0
            for userAgentEntry, queries in queryTypeDict.items():
                numberOfQueries = len(queries)
                if (numberOfQueries < args.threshold):
                    del queryTypeDict[userAgentEntry]
                else:
                    self.queryTypesCount[queryTypeEntry] += numberOfQueries

            if len(queryTypeDict) == 0:
                del self.queryTypes[queryTypeEntry]
                del self.queryTypesCount[queryTypeEntry]


    def writeOut(self):
        tooLong = 0

        with open(manualCheckupFolder + "readme.md", "w") as readmeFile:
            print("This directory contains all " + queryType + "-" + userAgent + "-Combinations above a threshold of " + str(args.threshold) + ".", file = readmeFile)
            print("count\t" + queryType + "\t" + userAgent + "-count", file = readmeFile)
            for queryTypeEntry, count in sorted(self.queryTypesCount.iteritems(), key = lambda (k, v): (v, k), reverse = True):
                print(str(count) + "\t" + queryTypeEntry + "\t" + str(len(self.queryTypes[queryTypeEntry])), file = readmeFile)

        for queryTypeEntry, queryTypeDict in self.queryTypes.iteritems():

            queryTypePath = preparePath(manualCheckupFolder, queryTypeEntry, tooLong)

            with open(queryTypePath + "info.txt", "w") as infoQueryTypeFile:
                print("count\t" + userAgent, file = infoQueryTypeFile)

                for i, (userAgentEntry, queries) in enumerate(sorted(queryTypeDict.iteritems(), key = lambda (k, v): (len(v), k), reverse = True)):

                    print(str(len(queries)) + "\t" + userAgentEntry, file = infoQueryTypeFile)

                    userAgentPath = preparePath(queryTypePath, userAgentEntry, tooLong)

                    for i, query in enumerate(queries):
                        with open(userAgentPath + "{}.query".format(i), "w") as queryFile:
                            queryFile.write(str(query))

        with open(pathBase + "newBots.tsv", "w") as newBots, gzip.open(tempForAnonymization + "QueryCnt01.tsv.gz", "w") as forAnonymization:
            print("queryType\tuserAgent\ttool\tversion\tcomment", file = newBots)
            print("uri_query\turi_path\tuser_agent\tts\tagent_type\thour\thttp_status", file = forAnonymization)
            for queryTypeEntry, queryTypeDict in self.queryTypes.iteritems():
                firstUserAgent = None
                for userAgentEntry in queryTypeDict:
                    if firstUserAgent == None:
                        firstUserAgent = userAgentEntry
                    print(queryTypeEntry + "\t" + userAgentEntry + "\t" + "not set\tnot set\t", file = newBots)
                if firstUserAgent != None:
                    encoded = urllib.quote_plus(queryTypeDict[firstUserAgent][0])
                    print("?query=" + encoded + "\tpath\tagent\ttime\ttype\thour\tstatus", file = forAnonymization)
                    queryTypeOrder.append(queryTypeEntry)

        mavenCall = ['mvn', 'exec:java@Anonymizer']

        mavenArguments = '-Dexec.args=-w ' + pathBase
        if args.logging:
            mavenArguments += " -l"
        mavenCall.append(mavenArguments)

        owd = os.getcwd()
        os.chdir("..")

        print("Starting anonymization of pre build query types for " + args.month + ".")

        if subprocess.call(['mvn', 'clean', 'package']) != 0:
            print("ERROR: Could not package the java application.")
            sys.exit(1)

        if subprocess.call(mavenCall) != 0:
            print("ERROR: Could not execute the java application. Check the logs "
                  + "for details or rerun this script with -l to generate logs.")
            sys.exit(1)

        os.chdir(owd)

        readerHandler = anonymizationReader()

        processdata.processDayAnonymous(readerHandler, 1, subfolder, monthsFolder + month)

handler = botClassification()

handler.prepare()

processdata.processMonth(handler, args.month, args.monthsFolder)

handler.threshold()

handler.writeOut()

shutil.rmtree(tempForAnonymization)
shutil.rmtree(pathBase + "anonymousRawData/")
