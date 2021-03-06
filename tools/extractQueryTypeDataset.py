import argparse
import config
import csv
import gzip
import os
import sys
from itertools import izip
from utility import utility

parser = argparse.ArgumentParser(
    description="Creates a subset of the raw log files and the processed log "
    + "files where for each present Unique Query an example Query is being " +
    "inserted")
parser.add_argument(
    "--monthsFolder",
    "-m",
    default=config.monthsFolder,
    type=str,
    help="the folder in which the months directory " + "are residing")
parser.add_argument(
    "--ignoreLock",
    "-i",
    help="Ignore locked file and execute anyways",
    action="store_true")
parser.add_argument(
    "month", type=str, help="the month which we're interested in")

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

if os.path.isfile(utility.addMissingSlash(args.monthsFolder) +
                  utility.addMissingSlash(args.month) + "locked") \
   and not args.ignoreLock:
    print "ERROR: The month " + args.month + " is being edited at the moment."
    + "Use -i if you want to force the execution of this script."
    sys.exit()

for monthName in args.month.split(","):
    subfolder = utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(monthName)
    subfolderUnique = subfolder + "queryTypeDataset/"

    processedFolderName = "processedLogData/"
    sourceFolderName = "rawLogData/"

    processedPrefix = processedFolderName + "QueryProcessedOpenRDF"
    sourcePrefix = sourceFolderName + "QueryCnt"

    if not os.path.exists(subfolderUnique):
        os.makedirs(subfolderUnique)
    if not os.path.exists(subfolderUnique + processedFolderName):
        os.makedirs(subfolderUnique + processedFolderName)
    if not os.path.exists(subfolderUnique + sourceFolderName):
        os.makedirs(subfolderUnique + sourceFolderName)

    usedQueryTypes = set()

    for i in xrange(1, 32):
        if not (os.path.exists(subfolder + processedPrefix + "%02d" % i + ".tsv.gz")
                and gzip.os.path.exists(subfolder + sourcePrefix + "%02d" % i + ".tsv.gz")):
            continue
        print "Working on %02d" % i
        with gzip.open(subfolder + processedPrefix + "%02d" % i + ".tsv.gz") as p, \
                gzip.open(subfolder + sourcePrefix + "%02d" % i + ".tsv.gz") as s, \
                gzip.open(subfolderUnique + processedPrefix + "%02d" % i
                          + ".tsv.gz", "w") as user_p, \
                gzip.open(subfolderUnique + sourcePrefix + "%02d" % i
                          + ".tsv.gz", "w") as user_s:
            pReader = csv.DictReader(p, delimiter="\t")
            sReader = csv.DictReader(s, delimiter="\t")

            pWriter = csv.DictWriter(user_p, None, delimiter="\t")
            sWriter = csv.DictWriter(user_s, None, delimiter="\t")

            for processed, source in izip(pReader, sReader):
                if pWriter.fieldnames is None:
                    ph = dict((h, h) for h in pReader.fieldnames)
                    pWriter.fieldnames = pReader.fieldnames
                    pWriter.writerow(ph)

                if sWriter.fieldnames is None:
                    sh = dict((h, h) for h in sReader.fieldnames)
                    sWriter.fieldnames = sReader.fieldnames
                    sWriter.writerow(sh)

                if (processed['QueryType'] not in usedQueryTypes):
                    pWriter.writerow(processed)
                    sWriter.writerow(source)
                    usedQueryTypes.add(processed['QueryType'])
