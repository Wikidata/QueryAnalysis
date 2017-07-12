import argparse
import calendar
import csv
import datetime
import glob
import os
import shutil
import subprocess
import sys

from utility import utility
from datetime import datetime

months = {'january': [1,31], 'february': [2,28], 'march': [3,31], 'april': [4,30], 'may': [5,31], 'june': [6,30], 'july': [7,30], 'august': [8,31], 'september': [9,30], 'october': [10,31], 'november': [11,30], 'december': [12,31]}

parser = argparse.ArgumentParser("This script extracts the raw log data (if it was not already done), processes them using the java application and unifies the query types.")
parser.add_argument("--monthsFolder", "-m", default="/a/akrausetud/months", type=str,
                    help="The folder in which the months directory are residing.")
parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and execute anyways", action="store_true")
parser.add_argument("--year", "-y", default=datetime.now().year, type=int, help="The year to be processed (default current year)")
parser.add_argument("month", type=str, help="The month to be processed")

# These are the field we extract from wmf.wdqs_extract that form the raw log data.
# They are not configurable via argument because the java program does not detect headers and thus depends on this specific order. 
fields = ["uri_query", "uri_path", "user_agent", "ts", "agent_type", "hour", "http_status"]

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

if os.path.isfile(utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(args.month) + "locked") and not args.ignoreLock:
    print "ERROR: The month " + args.month + " is being edited at the moment. Use -i if you want to force the execution of this script."
    sys.exit()
    
month = utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(args.month)

processedLogDataDirectory = month + "processedLogData/"
rawLogDataDirectory = month + "rawLogData/"
tempDirectory = rawLogDataDirectory + "temp/"

# If the month directory does not exist it is being created along with the directories for raw and processed log data.
if not os.path.exists(month):
    os.makedirs(month)
    os.makedirs(processedLogDataDirectory)
    os.makedirs(rawLogDataDirectory)
    
    arguments = ['hive', '-e']
    
    # For each day we send a command to hive that extracts all entries for this day (in the given month and year) and writes them to temporary files.
    for day in xrange(1, months[args.month][1] + 1):
        os.makedirs(tempDirectory)
        hive_call = 'insert overwrite local directory \'' + tempDirectory + '\' row format delimited fields terminated by \'\\t\' select '
        for field in fields:
            hive_call += field + " "
        hive_call += ' from wmf.wdqs_extract where uri_query<>"" and year=\'' + str(args.year) +  '\' and month=\'' + months[args.month][0] + '\' and day=\'' + day + '\''
        
        arguments.append(hive_call)
        if subprocess.call(arguments) != 0:
            print "ERROR: Raw data for month " + args.month + " does not exist but could not be extracted using hive."
            sys.exit(1)

        # The content of the temporary files is then copied to the actual raw log data file (with added headers)
        with open(rawLogDataDirectory + "QueryCnt" + day + ".tsv", "w") as dayfile:            
            dayfile.write(header)
            
            for filename in glob.glob('*'):
                with open(filename) as temp:
                    for line in temp:
                        dayfile.write(line)
    
        shutil.rmtree(tempDirectory)