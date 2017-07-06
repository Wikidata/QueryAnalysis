import argparse
import csv
import gzip
import os
from shutil import copyfile

import sys
from itertools import izip

parser = argparse.ArgumentParser(
	description="Creates a subset of the raw log files and the processed log files where #QueryType is USER")
parser.add_argument("--monthsFolder", "-m", default="/a/akrausetud/months", type=str,
					help="the folder in which the months directory are residing")
parser.add_argument("month", type=str, help="the month which we're interested in")

if (len(sys.argv[1:]) == 0):
	parser.print_help()
	parser.exit()

args = parser.parse_args()

os.chdir(args.monthsFolder + "/" + args.month)

subfolder = "userData/"

processedPrefix = "processedLogData/QueryProcessedOpenRDF"
sourcePrefix = "rawLogData/queryCnt"

if not os.path.exists(subfolder):
	os.makedirs(subfolder)
	os.makedirs(subfolder + "processedLogData")
	os.makedirs(subfolder + "rawLogData")

for i in xrange(1, 32):
	if not (os.path.exists(processedPrefix + "%02d" % i + ".tsv.gz") and os.path.exists(sourcePrefix + "%02d" % i + ".tsv")):
		continue
	print "Working on %02d" % i
	with gzip.open(processedPrefix + "%02d" % i + ".tsv.gz") as p, open(
							sourcePrefix + "%02d" % i + ".tsv") as s, gzip.open(
								subfolder + processedPrefix + "%02d" % i + ".tsv.gz", "w") as user_p, open(
								subfolder + sourcePrefix + "%02d" % i + ".tsv", "w") as user_s:
		pReader = csv.DictReader(p, delimiter="\t")
		sReader = csv.DictReader(s, delimiter="\t")

		pWriter = csv.DictWriter(user_p, None, delimiter="\t")
		sWriter = csv.DictWriter(user_s, None, delimiter="\t")

		for processed, source in izip(pReader, sReader):
			if pWriter.fieldnames is None:
				ph = dict((h, h) for h in pReader.fieldnames)
				ph['#uri_query'] = '#uri_query'
				ph['#hour'] = '#hour'
				ph['#agent_type'] = '#agent_type'
				ph['#ts'] = '#ts'
				pWriter.fieldnames = pReader.fieldnames
				pWriter.fieldnames.append('#uri_query')
				pWriter.fieldnames.append('#hour')
				pWriter.fieldnames.append('#agent_type')
				pWriter.fieldnames.append('#ts')
				pWriter.writerow(ph)

			if sWriter.fieldnames is None:
				sh = dict((h, h) for h in sReader.fieldnames)
				sWriter.fieldnames = sReader.fieldnames
				sWriter.writerow(sh)

			if (processed["#ToolName"] == "USER"):
				processed['#uri_query'] = source['uri_query']
				processed['#hour'] = source['hour']
				processed['#ts'] = source['ts']
				pWriter.writerow(processed)
				sWriter.writerow(source)