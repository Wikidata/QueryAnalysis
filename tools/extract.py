import argparse
import config
import csv
import gzip
import os
import sys

from itertools import izip
from utility import utility

class simpleDataset(object):
    subfolder = "simpleDataset/"
    p = None
    s = None
    pWriter = None
    sWriter = None

    def open(self, pReader, sReader, day):
        if self.p != None:
            self.p.close()
        if self.s != None:
            self.s.close()
        processedFolder = monthFolder + self.subfolder + config.processedFolderName
        if not os.path.exists(processedFolder):
            os.makedirs(processedFolder)

        sourceFolder = monthFolder + self.subfolder + config.sourceFolderName
        if not os.path.exists(sourceFolder):
            os.makedirs(sourceFolder)

        self.p = gzip.open(processedFolder + config.processedFilePrefix + "%02d" % day + ".tsv.gz", "w")
        self.s = gzip.open(sourceFolder +  config.sourceFilePrefix + "%02d" % day + ".tsv.gz", "w")

        self.pWriter = csv.DictWriter(self.p, None, delimiter="\t")
        self.sWriter = csv.DictWriter(self.s, None, delimiter="\t")

        if self.pWriter.fieldnames is None:
            ph = dict((h, h) for h in pReader.fieldnames)
            self.pWriter.fieldnames = pReader.fieldnames
            self.pWriter.writerow(ph)

        if self.sWriter.fieldnames is None:
            sh = dict((h, h) for h in sReader.fieldnames)
            self.sWriter.fieldnames = sReader.fieldnames
            self.sWriter.writerow(sh)

    def write(self, processed, source):
        self.pWriter.writerow(processed)
        self.sWriter.writerow(source)

    def close(self):
        self.pWriter.close()
        self.sWriter.close()

class uniqueDataset(simpleDataset):
    subfolder = "uniqueDataset/"

    def write(self, processed, source):
        if (processed['#First'] == "FIRST"):
            self.pWriter.writerow(processed)
            self.sWriter.writerow(source)

class userDataset(simpleDataset):
    subfolder = "userData/"

    def write(self, processed, source):
        if (processed["#SourceCategory"] == "USER"):
            self.pWriter.writerow(processed)
            self.sWriter.writerow(source)

class nonUserDataset(simpleDataset):
    subfolder = "nonUserData/"

    def write(self, processed, source):
        if (processed["#SourceCategory"] != "USER"):
            self.pWriter.writerow(processed)
            self.sWriter.writerow(source)

class status2xxDataset(simpleDataset):
    subfolder = "status2xx/"

    def write(self, processed, source):
        if (source["http_status"].startswith("2")):
            self.pWriter.writerow(processed)
            self.sWriter.writerow(source)

class status500Dataset(simpleDataset):
    subfolder = "status500/"

    def write(self, processed, source):
        if (source["http_status"] == ("500")):
            self.pWriter.writerow(processed)
            self.sWriter.writerow(source)

parser = argparse.ArgumentParser(
    description="Creates subsets of the raw and processed log files depending on choosen criteria."
)
parser.add_argument(
    "--monthsFolder",
    "-m",
    default=config.monthsFolder,
    type=str,
    help="The folder in which the month directories are residing."
)
parser.add_argument(
    "--ignoreLock",
    "-i",
    help="Ignore locked file and execute anyways",
    action="store_true"
)
parser.add_argument(
    "--uniqueDataset",
    "-q",
    help="A subset containing each unique query exactly once.",
    action="store_true"
)
parser.add_argument(
    "--userDataset",
    "-u",
    help="A subset containing only queries posed by users.",
    action="store_true"
)
parser.add_argument(
    "--nonUserDataset",
    "-n",
    help="A subset containing only queries posed by non-users.",
    action="store_true"
)
parser.add_argument(
    "--status2xxDataset",
    "-s2",
    help="A subset containing only queries with http status 2xx.",
    action="store_true"
)
parser.add_argument(
    "--status500Dataset",
    "-s5",
    help="A subset containing only queries with http status 500.",
    action="store_true"
)
parser.add_argument("months", type=str, help="The months of which subsets should be generated, separated by comma ',' if necessary.")

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

datasets = list()

if args.uniqueDataset:
    datasets.append(uniqueDataset())
if args.userDataset:
    datasets.append(userDataset())
if args.nonUserDataset:
    datasets.append(nonUserDataset())
if args.status2xxDataset:
    datasets.append(status2xxDataset())
if args.status500Dataset:
    datasets.append(status500Dataset())

for month in args.months.split(","):
    monthFolder = utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(month)

    if os.path.isfile(monthFolder + "locked") and not args.ignoreLock:
        print "ERROR: The month " + month + " is being edited at the moment. Use -i if you want to force the execution of this script."
        sys.exit()

    for i in xrange(1, 32):
        processed = monthFolder + config.processedPrefix + "%02d" % i + ".tsv.gz"
        source = monthFolder + config.sourcePrefix + "%02d" % i + ".tsv.gz"
        if not (os.path.exists(processed) and gzip.os.path.exists(source)):
            continue
        print "Working on %02d" % i
        with gzip.open(processed) as p, gzip.open(source) as s:
            pReader = csv.DictReader(p, delimiter="\t")
            sReader = csv.DictReader(s, delimiter="\t")

            for dataset in datasets:
                dataset.open(pReader, sReader, i)

            for processed, source in izip(pReader, sReader):
                for dataset in datasets:
                    dataset.write(processed, source)
