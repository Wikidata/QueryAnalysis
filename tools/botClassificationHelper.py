

import argparse
import os
import sys

from . import config
from . import fieldRanking

from .postprocess import processdata
from .utility import utility

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
                    + " Default is 40.", default = 40)
parser.add_argument("--switchKeys", "-s", help="Switch to searching for top N user agents and their top N query types.", action="store_true")
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

key1 = "QueryType"

key2 = "user_agent"

if args.switchKeys:
    key1, key2 = key2, key1
                            
def preparePath(path, directory, i):
    replacedDirectory = directory.replace("/", "SLASH")
    
    if len(replacedDirectory) > 140:
        replacedDirectory = replacedDirectory[:140] + str(i)
        i += 1
    
    fullPath = path + replacedDirectory + "/"
    
    if not os.path.exists(fullPath):
        os.makedirs(fullPath)
    
    return fullPath
    
class botClassification():
    
    firstKeys = dict()
    
    firstKeysCount = dict()
    
    actualNumber = 0
    
    def prepare(self):
        result = fieldRanking.fieldRanking(args.month, key1, args.monthsFolder, ignoreLock = args.ignoreLock, filterParams = args.filter)
        for i, (k, v) in enumerate(sorted(iter(result.items()), key=lambda k_v1: (k_v1[1], k_v1[0]), reverse = True)):
            self.actualNumber = i
            if i >= args.numberOfCombinations:
                break
            self.firstKeys[k] = dict()
            self.firstKeysCount[k] = v
    
    def handle(self, sparqlQuery, processed):
        if not filter.checkLine(processed):
            return
        
        firstKey = processed["#" + key1]
        if firstKey not in self.firstKeys:
            return
        
        firstKeyDict = self.firstKeys[firstKey]
        secondKey = processed["#" + key2]
        if secondKey not in firstKeyDict:
            firstKeyDict[secondKey] = list()
        firstKeyDict[secondKey].append(sparqlQuery)
        
    def writeOut(self):
        tooLong = 0
        
        with open(pathBase + "readme.md", "w") as readmeFile:
            print("This directory contains all top {}".format(self.actualNumber) + " " + key1 + "-" + key2 + "-Combinations.", file = readmeFile)
            print("count\t" + key1, file = readmeFile)
            for firstKey, count in sorted(iter(self.firstKeysCount.items()), key = lambda k_v2: (k_v2[1], k_v2[0]), reverse = True):
                print(str(count) + "\t" + firstKey, file = readmeFile)
        
        for firstKey, secondKeyDict in self.firstKeys.items():
                
            firstKeyPath = preparePath(pathBase, firstKey, tooLong)

            with open(firstKeyPath + "info.txt", "w") as infoFirstKeyFile:
                print("count\t" + key2, file = infoFirstKeyFile)
                
                for i, (secondKey, queries) in enumerate(sorted(iter(secondKeyDict.items()), key = lambda k_v: (len(k_v[1]), k_v[0]), reverse = True)):
                    if i >= args.numberOfCombinations:
                        break
                    
                    print(str(len(queries)) + "\t" + secondKey, file = infoFirstKeyFile)
                        
                    secondKeyPath = preparePath(firstKeyPath, secondKey, tooLong)
                        
                    for i, query in enumerate(queries):
                        with open(secondKeyPath + "{}.query".format(i), "w") as queryFile:
                            queryFile.write(str(query))
        
handler = botClassification()

handler.prepare()

processdata.processMonth(handler, args.month, args.monthsFolder)

if not os.path.exists(pathBase):
    os.makedirs(pathBase)
    
handler.writeOut()
        
        
