#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import sys

parser = argparse.ArgumentParser(
            description="Tool to combine TSV files that have one key column and one value column into one TSV")
parser.add_argument("files", type=str, nargs='+', help="List of files to merge")

args = parser.parse_args()

def getEmptyRecord():
    record = {}
    for filename in args.files:
        record[filename] = '0'
    return record

results = {}
for filename in args.files:
    with open(filename, 'rU') as f:
        for line in f:
            pieces = line.strip().split("\t")
            if len(pieces) == 2:
                if pieces[0] in results:
                    record = results[pieces[0]]
                else:
                    record = getEmptyRecord()
                record[filename] = pieces[1]
                results[pieces[0]] = record

sys.stdout.write("key\t")
for filename in args.files:
    sys.stdout.write("%s\t" % (filename))
sys.stdout.write("\n")


for record in results:
    sys.stdout.write("%s\t" % (record))
    for filename in args.files:
        sys.stdout.write("%s\t" % (results[record][filename]))
    sys.stdout.write("\n")

