import argparse
import os
import sys
from postprocess import processdata
from utility import utility
import config
import glob
import gzip
from itertools import izip
from random import random

parser = argparse.ArgumentParser(
    description="Creates a smaller testdata set for developing of the"
    + " current querys in this folder")
parser.add_argument("--monthsFolder", "-m", default=config.monthsFolder,
                    type=str, help="the folder in which the months"
                    + " directory are residing")
parser.add_argument("--ignoreLock", "-i",
                    help="Ignore locked file and execute anyways",
                    action="store_true")
parser.add_argument("month", type=str,
                    help="the month which we're interested in")
parser.add_argument("lines", type=int,
                    help="number of lines the testfiles should have")

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()
monthsFolder = args.monthsFolder
month = args.month


if os.path.isfile(utility.addMissingSlash(monthsFolder) +
                  utility.addMissingSlash(month) + "locked") \
   and not args.ignoreLock:
    print "ERROR: The month " + args.month + " is being edited at the moment."
    + " Use -i if you want to force the execution of this script."
    sys.exit()

# create new folder for the test data
os.makedirs("testData/processedLogData")
os.makedirs("testData/rawLogData")

for filename in glob.glob(monthsFolder + "/" + month
                          + "/processedLogData/" + processdata.processedPrefix
                          + "*" + processdata.processedSuffix):
    day = int(os.path.basename(filename)[len(processdata.processedPrefix):]
              [:-len(processdata.processedSuffix)])
    processedFileName = utility.addMissingSlash(monthsFolder) + month \
            + "/processedLogData/" + processdata.processedPrefix \
            + "%02d" % day + processdata.processedSuffix
    print "Working on: " + processedFileName

    with gzip.open(processedFileName) as p, \
            gzip.open("testData/processedLogData/" +
                      processdata.processedPrefix + "%02d" % day
                      + ".tsv.gz", "wb") as pc,\
            gzip.open("testData/rawLogData/" +
                      processdata.sourcePrefix +
                      "%02d" % day + ".tsv.gz", "wb") as sc:

        chancesSelected = float(args.lines) / float(sum(1 for line in p))

        print "Done counting lines, chance for selection is " \
                + str(chancesSelected)

        headerRow = True

        for processed, source in izip(gzip.open(processedFileName),
                                      gzip.open(monthsFolder + "/" + month +
                                                "/rawLogData/" +
                                                processdata.sourcePrefix +
                                                "%02d" % day + ".tsv.gz")):
            if(random() >= chancesSelected and not headerRow):
                continue
            elif headerRow:
                headerRow = False

            pc.write(processed)
            sc.write(source)
