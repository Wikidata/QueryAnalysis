import csv
import os

from shutil import copyfile

subfolder = "userData/"

processedPrefix = "QueryProcessedOpenRDF"
sourcePrefix = "QueryCnt"

if not os.path.exists(subfolder):
		os.makedirs(subfolder)
		
queryTypes = set()

for i in xrange(1, 2):
	with open(processedPrefix + "%02d" % i + ".tsv") as p, open(sourcePrefix + "%02d" % i + ".tsv") as s, open(subfolder + processedPrefix + "%02d" % i + ".tsv", "w") as user_p, open(subfolder + sourcePrefix + "%02d" % i + ".tsv", "w") as user_s:		
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
				queryTypes.add(processed["#QueryType"])
				
queryTypeFolder = "queryType/queryTypeFiles/"

if not os.path.exists(subfolder + queryTypeFolder):
	os.makedirs(subfolder + queryTypeFolder)

for queryType in queryTypes:
	original = queryTypeFolder + queryType + ".queryType"
	try:
		copyfile(original, "userData/" + original)
	except:
		print original + " does not exist."
	
