import argparse
import csv
import gzip
import os
import shutil
import sys

import pandas

import config

from utility import utility

parser = argparse.ArgumentParser(
    description="Sorts the raw log files by timestamp.")
parser.add_argument("--monthsFolder", "-m",
                    default=config.monthsFolder, type=str,
                    help="The folder in which the months directories are"
                    + " residing.")
parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and execute"
                    + " anyways", action="store_true")
parser.add_argument("month", type=str,
                    help="The month whose raw log files should be sorted.")


if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

sourcePrefix = config.sourcePrefix

if os.path.isfile(utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(args.month) + "locked") and not args.ignoreLock:
    print "ERROR: The month " + args.month + " is being edited at the moment. Use -i if you want to force the execution of this script."
    sys.exit()

os.chdir(utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(args.month))

for i in xrange(1, 32):
    filename = sourcePrefix + "%02d" % i
    filename_gzip = filename + ".tsv.gz"
    filename_tsv = filename + ".tsv"
    if not os.path.exists(filename_gzip):
        continue
    print "Working on %02d" % i
    with gzip.open(filename_gzip, 'rb') as input_file, open(filename_tsv, 'wb') as output_file:
        shutil.copyfileobj(input_file, output_file)
    os.remove(filename_gzip)
    df = pandas.read_csv(filename_tsv, sep="\t", header=0, index_col=0)
    df = df.sort_values(by=["ts"])
    df.to_csv(filename_tsv, sep="\t")
    with open (filename_tsv, 'rb') as input_file, gzip.open(filename_gzip, 'wb') as output_file:
        shutil.copyfileobj(input_file, output_file)
    os.remove(filename_tsv)
