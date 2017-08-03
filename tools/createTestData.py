import argparse
import os
import sys
from postprocess import processdata
from utility import utility
import config
import csv
import glob
import gzip
from itertools import izip
from random import random
from pprint import pprint

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
os.makedirs(monthsFolder + "/testData")

for filename in glob.glob(monthsFolder + "/" + month
                          + "/processedLogData/" + processdata.processedPrefix
                          + "*" + processdata.processedSuffix):
    day = os.path.basename(filename)[len(processdata.processedPrefix):]\
            [:-len(processdata.processedSuffix)]
    processedFileName = utility.addMissingSlash(monthsFolder) + month \
            + "/processedLogData/" + processdata.processedPrefix \
            + "%02d" % day + processdata.processedSuffix
    print "Working on: " + processedFileName
    with gzip.open(processedFileName) as p, \
            gzip.open(monthsFolder + "/" + month + "/rawLogData/"
                      + processdata.sourcePrefix
                      + "%02d" % day + ".tsv.gz") as s,\
            gzip.open(monthsFolder + "/testData/processedLogData/" +
                      processdata.processedPrefix + "%02d" % day +
                      processedFileName) as pc,\
            gzip.open(monthsFolder + "/testData/rawLogData/" +
                      processdata.sourcePrefix +
                      "%02d" % day + ".tsv.gz") as sc:

        chancesSelected = 1000 / sum(1 for line in p)

        for processed, source in izip(p, s):
            if(random() > chancesSelected):
                continue
            pc.write(p)
            sc.write(s)
