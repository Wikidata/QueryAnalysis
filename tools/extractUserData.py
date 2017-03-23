import csv
import os
from gtk._gtk import Item

pathBase = "../inputData/"

if not os.path.exists(pathBase + "userData"):
		os.makedirs(pathBase + "userData")

for i in xrange(1, 2):
	with open(pathBase + "QueryProcessedOpenRDF" + "%02d" % i + ".tsv") as p, open(pathBase + "QueryCnt" + "%02d" % i + ".tsv") as s, open(pathBase + "/userData/QueryProcessedOpenRDF" + "%02d" % i + ".tsv", "w") as user_p, open(pathBase + "/userData/QueryCnt" + "%02d" % i + ".tsv", "w") as user_s:		
		pReader = csv.DictReader(p, delimiter="\t")
		sReader = csv.DictReader(s, delimiter="\t")
		
		pWriter = csv.DictWriter(user_p, None, delimiter = "\t")
		sWriter = csv.DictWriter(user_s, None, delimiter = "\t")
		
		for processed, source in zip(pReader, sReader):
			if pWriter.fieldnames is None:
				ph = dict((h, h) for h in pReader.fieldnames)
				pWriter.fieldnames = pReader.fieldnames
				pWriter.writerow(ph)

			if sWriter.fieldnames is None:
				sh = dict((h, h) for h in sReader.fieldnames)
				sWriter.fieldnames = sReader.fieldnames
				sWriter.writerow(sh)
			
			if (processed["#ToolName"] == "0"):
				pWriter.writerow(processed)
				sWriter.writerow(source)

	
