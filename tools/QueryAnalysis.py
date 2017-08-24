import argparse
import calendar
from datetime import datetime
import glob
import os
import shutil
import subprocess
import sys
import gzip
import unifyQueryTypes
from utility import utility
import config

months = {'january': [1, 31],
          'february': [2, 28],
          'march': [3, 31],
          'april': [4, 30],
          'may': [5, 31],
          'june': [6, 30],
          'july': [7, 30],
          'august': [8, 31],
          'september': [9, 30],
          'october': [10, 31],
          'november': [11, 30],
          'december': [12, 31]}

parser = argparse.ArgumentParser("This script extracts the raw log data (if "
                                 + "it was not already done), processes them"
                                 + " using the java application and unifies "
                                 + "the query types.")
parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and "
                    + "execute anyways", action="store_true")
parser.add_argument("--threads", "-t", default=7, type=int, help="The number "
                    + "of threads to run the java program with (default 7).")
parser.add_argument("--logging", "-l", help="Enables file logging.",
                    action="store_true")
parser.add_argument("--noBotMetrics", "-b", help="Disables metric calculation"
                    + " for bot queries.", action="store_true")
parser.add_argument("--noDynamicQueryTypes", "-d", help="Disables dynamic "
                    + "generation of query types.", action="store_true")
parser.add_argument("--noGzipOutput", "-g", help="Disables gzipping of the "
                    + "output files.", action="store_true")
parser.add_argument("--noExampleQueriesOutput", "-e", help="Disables the "
                    + "matching of example queries.", action="store_true")
parser.add_argument("--referenceDirectory", "-r",
                    default=config.queryReferenceDirectory,
                    type=str,
                    help="The directory with the reference query types.")
parser.add_argument("--fdupesExecutable", "-f",
                    default=config.fdupesExecutable, type=str,
                    help="The location of the fdupes executable.")
parser.add_argument("--monthsFolder", "-m", default=config.monthsFolder,
                    type=str,
                    help="The folder in which the months directory are "
                    + "residing.")
parser.add_argument("--year", "-y", default=datetime.now().year, type=int,
                    help="The year to be processed (default current year).")
parser.add_argument("months", type=str, help="The months to be processed")

# These are the field we extract from wmf.wdqs_extract that form the raw
# log data. They are not configurable via argument because the java program
# does not detect headers and thus depends on this specific order.
fields = ["uri_query", "uri_path", "user_agent", "ts", "agent_type",
          "hour", "http_status"]

header = ""
for field in fields:
    header += field + "\t"
header = header[:-1] + "\n"

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

if calendar.isleap(args.year):
    months['february'][1] = 29

for monthName in args.months.split(","):
    if os.path.isfile(utility.addMissingSlash(args.monthsFolder)
                      + utility.addMissingSlash(monthName) + "locked") \
       and not args.ignoreLock:
        print "ERROR: The month " + monthName + " is being edited at the " \
        + "moment. Use -i if you want to force the execution of this script."
        sys.exit()

    month = utility.addMissingSlash(os.path.abspath(utility.addMissingSlash(args.monthsFolder)
                            + utility.addMissingSlash(monthName)))

    processedLogDataDirectory = month + "processedLogData/"
    rawLogDataDirectory = month + "rawLogData/"
    tempDirectory = rawLogDataDirectory + "temp/"

    # If the month directory does not exist it is being created along with
    # the directories for raw and processed log data.
    if not os.path.exists(month):
        print("Starting data extraction from wmf.wdqs_extract for "
              + monthName + ".")

        os.makedirs(month)
        os.makedirs(processedLogDataDirectory)
        os.makedirs(rawLogDataDirectory)

        arguments = ['hive', '-e']

        # For each day we send a command to hive that extracts all entries for
        # this day (in the given month and year) and writes them to temporary
        # files.
        for day in xrange(1, months[monthName][1] + 1):
            os.makedirs(tempDirectory)
            hive_call = 'insert overwrite local directory \'' + tempDirectory \
                    + '\' row format delimited fields terminated ' \
                    + 'by \'\\t\' select '

            # We add all the fields to the request
            for field in fields:
                hive_call += field + ", "
            hive_call = hive_call[:-2] + " "

            hive_call += ' from wmf.wdqs_extract where uri_query<>"" ' \
                    + 'and year=\'' + str(args.year) +  '\' and month=\'' \
                    + str(months[monthName][0]) + '\' and day=\'' + str(day) + '\''

            arguments.append(hive_call)
            if subprocess.call(arguments) != 0:
                print("ERROR: Raw data for month " + monthName + " does not "
                      + "exist but could not be extracted using hive.")
                sys.exit(1)

            # The content of the temporary files is then copied to the actual
            # raw log data file (with added headers)
            with gzip.open(rawLogDataDirectory + "QueryCnt"
                           + "%02d"%day  + ".tsv.gz", "wb") as dayfile:
                dayfile.write(header)

                for filename in glob.glob(tempDirectory + '*'):
                    with open(filename) as temp:
                        for line in temp:
                            dayfile.write(line)

            shutil.rmtree(tempDirectory)

    # We build the call to execute the java application with the location of
    # the files, the number of threads to use and any optional arguments needed

    mavenCall = ['mvn', 'exec:java']

    mavenArguments = '-Dexec.args=-w ' + month + ' -n ' + str(args.threads)

    if args.logging:
        mavenArguments += " -l"
        if args.noBotMetrics:
            mavenArguments += " -b"
            if args.noDynamicQueryTypes:
                mavenArguments += " -d"
                if args.noGzipOutput:
                    mavenArguments += " -g"
                    if args.noExampleQueriesOutput:
                        mavenArguments += " -e"

    mavenCall.append(mavenArguments)

    owd = os.getcwd()
    os.chdir("..")

    print "Starting data processing using QueryAnalysis for " + monthName + "."

    if subprocess.call(['mvn', 'clean', 'package']) != 0:
        print "ERROR: Could not package the java application."
        sys.exit(1)

    if subprocess.call(mavenCall) != 0:
        print("ERROR: Could not execute the java application. Check the logs "
              + "for details or rerun this script with -l to generate logs.")
        sys.exit(1)

    os.chdir(owd)

    # Finally we call the query type unification
    # (see python unifyQueryTypes.py for details)

    print "Starting to unify the query types for " + monthName + "."

    unifyQueryTypes.unifyQueryTypes(monthName,
                                    args.monthsFolder,
                                    args.referenceDirectory,
                                    args.fdupesExecutable)
