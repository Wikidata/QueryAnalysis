import csv

import os

subfolder = "benchmarkFiles/"

processedPrefix = "QueryProcessedOpenRDF"
sourcePrefix = "QueryCnt"

lineNumber = 5000

if not os.path.exists(subfolder):
	os.makedirs(subfolder)

queryTypes = set()

for i in xrange(1, 2):
	with open(processedPrefix + "%02d" % i + ".tsv") as p, open(sourcePrefix + "%02d" % i + ".tsv") as s, open(
									subfolder + processedPrefix + "%02d" % i + ".tsv", "w") as user_p, open(
								subfolder + sourcePrefix + "%02d" % i + ".tsv", "w") as user_s:
		pReader = csv.DictReader(p, delimiter="\t")
		sReader = csv.DictReader(s, delimiter="\t")

		pWriter = csv.DictWriter(user_p, None, delimiter="\t")
		sWriter = csv.DictWriter(user_s, None, delimiter="\t")

		i = 0

		for processed, source in zip(pReader, sReader):

			if pWriter.fieldnames is None:
				ph = dict((h, h) for h in pReader.fieldnames)
				pWriter.fieldnames = pReader.fieldnames
				pWriter.writerow(ph)

			if sWriter.fieldnames is None:
				sh = dict((h, h) for h in sReader.fieldnames)
				sWriter.fieldnames = sReader.fieldnames
				sWriter.writerow(sh)

			if (processed["#QueryType"] in queryTypes):
				continue
			else:
				queryTypes.add(processed["#QueryType"])
				pWriter.writerow(processed)
				sWriter.writerow(source)
				i += 1

			if i > lineNumber:
				break
