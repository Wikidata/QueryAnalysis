from __future__ import print_function

import argparse
import os
import sys

import config
import fieldRanking

from postprocess import processdata
from utility import utility

parser = argparse.ArgumentParser(description = "This script searches for all queries for the top N query types and their top N user agents.")
parser.add_argument("--monthsFolder", "-m", default=config.monthsFolder,
                    type=str, help="The folder in which the months directory "
                    + "are residing.")
parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and execute"
                    + " anyways", action="store_true")
parser.add_argument("--outputPath", "-o", type=str, help="The path where the "
                    + "output files should be generated.")
parser.add_argument("--filter", "-f", default="", type=str, help="Constraints "
                    + "used to limit the lines used to generate the output."
                    + " Default filter is Valid=^VALID$."
                    + " Enter as <metric>=<regex>,<othermetric>/<regex> (e.g."
                    + " QueryType=wikidataLastModified,ToolName=^USER$)"
                    + " NOTE: If you use this option you should probably also"
                    + " set the --outputPath to some value other than the "
                    + "default.")
parser.add_argument("--numberOfCombinations", "-n", type=int, help="The number N for which combinations should be generated."
                    + " Default is 100.", default = 100)
parser.add_argument("month", type=str,
                    help="The month for which the ranking should be " 
                    +"generated.")

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
    
pathBase = monthsFolder + month + "botClassificationHelper/"

if args.outputPath is not None:
    pathBase = args.outputPath

filter = utility.filter()

filter.setup(args.filter)
    
class botClassification():
    
    queryTypes = dict()
    
    queryTypesCount = dict()
    
    actualNumber = 0
    
    def prepare(self):
        result = fieldRanking.fieldRanking(args.month, "QueryType", args.monthsFolder, ignoreLock = args.ignoreLock, filterParams = args.filter)
        for i, (k, v) in enumerate(sorted(result.iteritems(), key=lambda (k, v): (v, k), reverse = True)):
            if i >= args.numberOfCombinations:
                self.actualNumber = i
                break
            self.queryTypes[k] = dict()
            self.queryTypesCount[k] = v
    
    def handle(self, sparqlQuery, processed):
        if not filter.checkLine(processed):
            return
        
        queryType = processed["#QueryType"]
        if queryType not in self.queryTypes:
            return
        
        queryTypeDict = self.queryTypes[queryType]
        userAgent = processed["#user_agent"]
        if userAgent not in queryTypeDict:
            queryTypeDict[userAgent] = list()
        queryTypeDict[userAgent].append(sparqlQuery)
        
    def writeOut(self):
        tooLong = 0
        
        with open(pathBase + "readme.md", "w") as readmeFile:
            print("This directory contains all top {} queryType-userAgent-Combinations.".format(self.actualNumber), file = readmeFile)
            print("count\tQueryType", file = readmeFile)
            for queryType, count in sorted(self.queryTypesCount.iteritems(), key = lambda (k, v): (v, k), reverse = True):
                print(str(count) + "\t" + queryType, file = readmeFile)
        
        for queryType, userAgentsDict in self.queryTypes.iteritems():

            queryTypePath = pathBase + queryType + "/"
            if not os.path.exists(queryTypePath):
                os.makedirs(queryTypePath)

            with open(queryTypePath + "info.txt", "w") as infoQueryTypeFile:
                print("count\tuser_agent", file = infoQueryTypeFile)
                
                for i, (userAgent, queries) in enumerate(sorted(userAgentsDict.iteritems(), key = lambda (k, v): (len(v), k), reverse = True)):
                    if i >= args.numberOfCombinations:
                        break
                    
                    print(str(len(queries)) + "\t" + userAgent, file = infoQueryTypeFile)
                    
                    replacedUserAgent = userAgent.replace("/", "SLASH")
                    
                    if len(replacedUserAgent) > 140:
                        replacedUserAgent = replacedUserAgent[:140] + str(tooLong)
                        tooLong += 1
                    
                    userAgentPath = queryTypePath + replacedUserAgent + "/"
                
                    if not os.path.exists(userAgentPath):
                        os.makedirs(userAgentPath)
                        
                    for i, query in enumerate(queries):
                        with open(userAgentPath + "{}.query".format(i), "w") as queryFile:
                            queryFile.write(str(query))
        
handler = botClassification()

handler.prepare()

processdata.processMonth(handler, args.month, args.monthsFolder)

if not os.path.exists(pathBase):
    os.makedirs(pathBase)
    
handler.writeOut()
        
        