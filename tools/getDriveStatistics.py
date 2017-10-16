import argparse
import os
import subprocess
import sys

from utility import utility
import config

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

for monthName in args.months.split(","):
    if os.path.isfile(utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(monthName) + "locked") and not args.ignoreLock:
        print "ERROR: The month " + monthName + " is being edited at the moment. Use -i if you want to force the execution of this script."
        continue

    month = utility.addMissingSlash(monthName)

    for secondKey, secondFolder in {"user":"userData", "nonUser":"nonUserData"}.iteritems():
        for thirdKey, thirdFolder in {"all":"", "unique":"uniqueQueryDataset", "queryType":"queryTypeDataset"}.iteritems():
            for script, scriptFolder in {"getSparqlStatistic.py":"sparqlFeatures", "operatorUsageStatistic.py":"operatorUsage", "generalStat.py":"generalStats"}.iteritems():
                folder = utility.addMissingSlash(statisticsSubfolder + scriptFolder)
                if not os.path.exists(folder):
                    os.makedirs(folder)
                filename = monthName.strip("/").replace("/", "SLASH") + "#" + secondKey + "#" + thirdKey
                print "Working with " + script + " on " + filename
                with open(folder + filename, "w") as f:
                    monthFolder = month + secondFolder + "/" + thirdFolder + "/"
                    if subprocess.call(['python', script, monthFolder.strip("/"), '-m', args.monthsFolder, '-p', monthName + "\n" + secondKey + "\n" + thirdKey], stdout = f) != 0:
                        print "ERROR: Could not calculate " + filename
