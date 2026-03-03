import argparse
import os
import subprocess
import sys

from utility import utility
import config
import fieldRanking
import xyMapping

os.nice(19)

parser = argparse.ArgumentParser("This script executes multiple evaluation scripts.")
parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and "
                    + "execute anyways", action="store_true")
parser.add_argument("--monthsFolder", "-m", default=config.monthsFolder,
                    type=str,
                    help="The folder in which the months directory are "
                    + "residing.")
parser.add_argument("months", type=str, help="The months to be processed")

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

monthsFolder = utility.addMissingSlash(args.monthsFolder)
statisticsSubfolder = monthsFolder + "statistics/"
if not os.path.exists(statisticsSubfolder):
    os.makedirs(statisticsSubfolder)

def fieldRankingOn(monthFolder, metric, filename):
    print "Working with fieldRanking " + metric + " on " + filename
    fieldRanking.fieldRanking(monthFolder, metric, monthsFolder = args.monthsFolder, outputPath = statisticsSubfolder + metric + "_Ranking", outputFilename = filename, writeOut = True, notifications = False)

def xyMappingOn(monthFolder, metricOne, metricTwo, filename, nosplitOne = False, nosplitTwo = False):
    print "Working with xyMapping " + metricOne + " " + metricTwo + " on " + filename
    xyMapping.xyMapping(monthFolder, metricOne, metricTwo, monthsFolder = args.monthsFolder, outputPath = statisticsSubfolder + metricOne + "_" + metricTwo, outputFilename = filename, nosplittingOne = nosplitOne, nosplittingTwo = nosplitTwo, writeOut = True, notifications = False)

for monthName in args.months.split(","):
    cleanMonthName = monthName.strip("/").replace("/", "SLASH")

    if os.path.isfile(utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(monthName) + "locked") and not args.ignoreLock:
        print "ERROR: The month " + monthName + " is being edited at the moment. Use -i if you want to force the execution of this script."
        continue

    month = utility.addMissingSlash(monthName)

    for secondKey, secondFolder in {"user":"userData", "nonUser":"nonUserData"}.iteritems():
        for thirdKey, thirdFolder in {"all":"", "queryType":"queryTypeDataset"}.iteritems():
            monthFolder = month + secondFolder + "/" + thirdFolder + "/"
            monthFolder = monthFolder.strip("/")

            filename = cleanMonthName + "#" + secondKey + "#" + thirdKey
            fieldRankingOn(monthFolder, "Predicates", filename)
            fieldRankingOn(monthFolder, "Categories", filename)
            fieldRankingOn(monthFolder, "TripleCountWithService", filename)
            fieldRankingOn(monthFolder, "TripleCountWithoutService", filename)
            fieldRankingOn(monthFolder, "ToolName", filename)
            fieldRankingOn(monthFolder, "NonSimplePropertyPaths", filename)
            fieldRankingOn(monthFolder, "PrimaryLanguage", filename)
            fieldRankingOn(monthFolder, "ServiceCalls", filename)
            if thirdKey is not "queryType":
                fieldRankingOn(monthFolder, "QueryType", filename)
            xyMappingOn(monthFolder, "UsedSparqlFeatures", "QuerySize", filename)
            for script, scriptFolder in {"getSparqlStatistic.py":"sparqlFeatures", "operatorUsageStatistic.py":"operatorUsage", "generalStat.py":"generalStats"}.iteritems():
                folder = utility.addMissingSlash(statisticsSubfolder + scriptFolder)
                if not os.path.exists(folder):
                    os.makedirs(folder)
                print "Working with " + script + " on " + filename
                with open(folder + filename, "w") as f:
                    if subprocess.call(['python', script, monthFolder, '-m', args.monthsFolder, '-p', monthName + "\n" + secondKey + "\n" + thirdKey], stdout = f) != 0:
                        print "ERROR: Could not calculate " + filename
