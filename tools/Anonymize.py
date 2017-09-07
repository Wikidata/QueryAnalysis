import argparse
import config
import os
import subprocess
import sys

from utility import utility

parser = argparse.ArgumentParser("This script creates an anonymous dataset from the rawLogData.")
parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and "
                    + "execute anyways", action="store_true")
parser.add_argument("--threads", "-t", default=7, type=int, help="The number "
                    + "of threads to run the java program with (default 7).")
parser.add_argument("--monthsFolder", "-m", default=config.monthsFolder,
                    type=str,
                    help="The folder in which the months directory are "
                    + "residing.")
parser.add_argument("months", type=str, help="The months to be processed")

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

for monthName in args.months.split(","):

    mavenCall = ['mvn', 'exec:java@Anonymizer']

    month = utility.addMissingSlash(os.path.abspath(utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(monthName)))
    mavenArguments = '-Dexec.args=-w ' + month + ' -n ' + str(args.threads)
    mavenCall.append(mavenArguments)

    owd = os.getcwd()
    os.chdir("..")

    print "Starting data processing using Anonymizer for " + monthName + "."

    if subprocess.call(['mvn', 'clean', 'package']) != 0:
        print "ERROR: Could not package the java application."
        sys.exit(1)

    if subprocess.call(mavenCall) != 0:
        print("ERROR: Could not execute the java application. Check the logs "
              + "for details or rerun this script with -l to generate logs.")
        sys.exit(1)

    os.chdir(owd)