import argparse
import csv
import gzip
import os
import sys

from dateutil.parser import parse
from itertools import izip

import config
from utility import utility

parser = argparse.ArgumentParser(
    description="Creates a two subsets of the raw log files and the processed log files that would have been cached / not been cached.")
parser.add_argument("--monthsFolder", "-m", default=config.monthsFolder,
                    type=str, help="the folder in which the months directory "
                    + "are residing")
parser.add_argument("--ignoreLock", "-i",
                    help="Ignore locked file and execute anyways",
                    action="store_true")
parser.add_argument("month", type=str,
                    help="the month which we're interested in")

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

if os.path.isfile(utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(args.month) + "locked") and not args.ignoreLock:
    print "ERROR: The month " + args.month + " is being edited at the moment. Use -i if you want to force the execution of this script."
    sys.exit()

os.chdir(utility.addMissingSlash(args.monthsFolder)
         + utility.addMissingSlash(args.month))

subfolderCached = "cachedData/"
subfolderUncached = "uncachedData/"

processedPrefix = config.processedPrefix
sourcePrefix = config.sourcePrefix

if not os.path.exists(subfolderCached):
    os.makedirs(subfolderCached)
    os.makedirs(subfolderCached + "processedLogData")
    os.makedirs(subfolderCached + "rawLogData")

if not os.path.exists(subfolderUncached):
    os.makedirs(subfolderUncached)
    os.makedirs(subfolderUncached + "processedLogData")
    os.makedirs(subfolderUncached + "rawLogData")

for i in xrange(1, 32):
    if not (os.path.exists(processedPrefix + "%02d" % i + ".tsv.gz")
            and gzip.os.path.exists(sourcePrefix + "%02d" % i + ".tsv.gz")):
        continue
    print "Working on %02d" % i
    with gzip.open(processedPrefix + "%02d" % i + ".tsv.gz") as p, \
            gzip.open(sourcePrefix + "%02d" % i + ".tsv.gz") as s, \
            gzip.open(subfolderCached + processedPrefix + "%02d" % i   + ".tsv.gz", "w") as cached_p, \
            gzip.open(subfolderCached + sourcePrefix + "%02d" % i + ".tsv.gz", "w") as cached_s, \
            gzip.open(subfolderUncached + processedPrefix + "%02d" % i   + ".tsv.gz", "w") as uncached_p, \
            gzip.open(subfolderUncached + sourcePrefix + "%02d" % i + ".tsv.gz", "w") as uncached_s:
        pReader = csv.DictReader(p, delimiter="\t")
        sReader = csv.DictReader(s, delimiter="\t")

        cachedpWriter = csv.DictWriter(cached_p, None, delimiter="\t")
        cachedsWriter = csv.DictWriter(cached_s, None, delimiter="\t")

        uncachedpWriter = csv.DictWriter(uncached_p, None, delimiter="\t")
        uncachedsWriter = csv.DictWriter(uncached_s, None, delimiter="\t")

        cache = dict()

        lasttime = None

        for processed, source in izip(pReader, sReader):
            if cachedpWriter.fieldnames is None:
                ph = dict((h, h) for h in pReader.fieldnames)
                cachedpWriter.fieldnames = pReader.fieldnames
                cachedpWriter.writerow(ph)

            if uncachedpWriter.fieldnames is None:
                ph = dict((h, h) for h in pReader.fieldnames)
                uncachedpWriter.fieldnames = pReader.fieldnames
                uncachedpWriter.writerow(ph)

            if cachedsWriter.fieldnames is None:
                sh = dict((h, h) for h in sReader.fieldnames)
                cachedsWriter.fieldnames = sReader.fieldnames
                cachedsWriter.writerow(sh)

            if uncachedsWriter.fieldnames is None:
                sh = dict((h, h) for h in sReader.fieldnames)
                uncachedsWriter.fieldnames = sReader.fieldnames
                uncachedsWriter.writerow(sh)

            uri_query = source["uri_query"]
            timestamp = parse(source["ts"])

            if lasttime == None:
                lasttime = timestamp
                cache[uri_query] = timestamp
                uncachedpWriter.writerow(processed)
                uncachedsWriter.writerow(source)
            else:
                if lasttime < timestamp:
                    for k, v in cache.items():
                        if (timestamp - v).total_seconds() / 60 > 5.0:
                            del cache[k]
                if uri_query in cache:
                    cachedpWriter.writerow(processed)
                    cachedsWriter.writerow(source)
                else:
                    uncachedpWriter.writerow(processed)
                    uncachedsWriter.writerow(source)
                    cache[uri_query] = timestamp
