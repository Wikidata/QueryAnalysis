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
    + "files where for each present QueryType an example Query is being " +
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

os.chdir(
    utility.addMissingSlash(args.monthsFolder) +
    utility.addMissingSlash(args.month))

subfolderUser = "uniqueQueryDatasetUser/"
subfolderNonUser = "uniqueQueryDatasetNonUser/"

processedPrefix = "processedLogData/QueryProcessedOpenRDF"
sourcePrefix = "rawLogData/QueryCnt"

if not os.path.exists(subfolderUser):
    os.makedirs(subfolderUser)
    os.makedirs(subfolderUser + "processedLogData")
    os.makedirs(subfolderUser + "rawLogData")

if not os.path.exists(subfolderNonUser):
    os.makedirs(subfolderNonUser)
    os.makedirs(subfolderNonUser + "processedLogData")
    os.makedirs(subfolderNonUser + "rawLogData")

userToFind = list()
nonUserToFind = list()

firstRun = True

for value in xrange(0,2):
    if ((not firstRun) and (len(userToFind) == 0) and (len(nonUserToFind) == 0)):
        break

    for i in xrange(1, 32):
        if not (os.path.exists(processedPrefix + "%02d" % i + ".tsv.gz")
                and gzip.os.path.exists(sourcePrefix + "%02d" % i + ".tsv.gz")):
            continue
        print "Working on %02d" % i
        if firstRun:
            parameter = "w"
        else:
            parameter = "a"
        with gzip.open(processedPrefix + "%02d" % i + ".tsv.gz") as p, \
                gzip.open(sourcePrefix + "%02d" % i + ".tsv.gz") as s, \
                gzip.open(subfolderUser + processedPrefix + "%02d" % i
                          + ".tsv.gz", parameter) as user_p, \
                gzip.open(subfolderUser + sourcePrefix + "%02d" % i
                          + ".tsv.gz", parameter) as user_s, \
    			gzip.open(subfolderNonUser + processedPrefix + "%02d" % i
                          + ".tsv.gz", parameter) as non_user_p, \
                gzip.open(subfolderNonUser + sourcePrefix + "%02d" % i
                          + ".tsv.gz", parameter) as non_user_s:
            pReader = csv.DictReader(p, delimiter="\t")
            sReader = csv.DictReader(s, delimiter="\t")

            pUserWriter = csv.DictWriter(user_p, None, delimiter="\t")
            sUserWriter = csv.DictWriter(user_s, None, delimiter="\t")

            pNonUserWriter = csv.DictWriter(non_user_p, None, delimiter="\t")
            sNonUserWriter = csv.DictWriter(non_user_s, None, delimiter="\t")

            ph = dict((h, h) for h in pReader.fieldnames)
            pUserWriter.fieldnames = pReader.fieldnames

            sh = dict((h, h) for h in sReader.fieldnames)
            sUserWriter.fieldnames = sReader.fieldnames

            ph = dict((h, h) for h in pReader.fieldnames)
            pNonUserWriter.fieldnames = pReader.fieldnames

            sh = dict((h, h) for h in sReader.fieldnames)
            sNonUserWriter.fieldnames = sReader.fieldnames

            if firstRun:
                pUserWriter.writerow(ph)
                sUserWriter.writerow(sh)

                pNonUserWriter.writerow(ph)
                sNonUserWriter.writerow(sh)

            for processed, source in izip(pReader, sReader):
                originalId = processed['#OriginalId']

                if (processed['#SourceCategory'] == "USER"):
                    if (originalId in userToFind):
                        userToFind.remove(originalId)
                        pUserWriter.writerow(processed)
                        sUserWriter.writerow(source)
                    elif (processed['#First'] == "FIRST" and firstRun):
                        nonUserToFind.append(processed['#UniqueId'])
                        pUserWriter.writerow(processed)
                        sUserWriter.writerow(source)
                else:
                    if (originalId in nonUserToFind):
                        nonUserToFind.remove(originalId)
                        pNonUserWriter.writerow(processed)
                        sNonUserWriter.writerow(source)
                    elif (processed['#First'] == "FIRST" and firstRun):
                        userToFind.append(processed['#UniqueId'])
                        pNonUserWriter.writerow(processed)
                        sNonUserWriter.writerow(source)
    firstRun = False
