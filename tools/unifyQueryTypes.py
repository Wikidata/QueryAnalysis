import os
import re
import csv

from itertools import izip

queryTypeSubfolder = "queryType/queryTypeFiles/"

subfolder = "temp/"

if not os.path.exists(subfolder):
    os.makedirs(subfolder)

duplicatesFile = "duplicates.txt"

processedPrefix = "QueryProcessedOpenRDF"
sourcePrefix = "queryCnt"

os.system("./fdupes " + queryTypeSubfolder + " > " + duplicatesFile)

replacementDict = dict()

def lineCleanUp(line):
    return re.sub(".queryType\n", '', re.sub(queryTypeSubfolder, '', line))

with open(duplicatesFile) as dupes:
    
    newBlock = True
    
    currentQueryType = ""
    
    for line in dupes:
        if line == "\n":
            newBlock = True
            continue
        if newBlock:
            currenctQueryType = lineCleanUp(line)
            newBlock = False
            continue
        replacementDict[lineCleanUp(line)] = currenctQueryType
        
os.remove(duplicatesFile)

columnIdentifier = "#QueryType"

for i in xrange(1, 5):
    print "Working on %02d" % i
    with open(processedPrefix + "%02d" % i + ".tsv") as p, open(sourcePrefix + "%02d" % i + ".tsv") as s, open(
                                    subfolder + processedPrefix + "%02d" % i + ".tsv", "w") as user_p, open(
                                subfolder + sourcePrefix + "%02d" % i + ".tsv", "w") as user_s:
        pReader = csv.DictReader(p, delimiter="\t")
        sReader = csv.DictReader(s, delimiter="\t")

        pWriter = csv.DictWriter(user_p, None, delimiter="\t")
        sWriter = csv.DictWriter(user_s, None, delimiter="\t")

        for processed, source in izip(pReader, sReader):
            if pWriter.fieldnames is None:
                ph = dict((h, h) for h in pReader.fieldnames)
                pWriter.fieldnames = pReader.fieldnames
                pWriter.writerow(ph)

            if sWriter.fieldnames is None:
                sh = dict((h, h) for h in sReader.fieldnames)
                sWriter.fieldnames = sReader.fieldnames
                sWriter.writerow(sh)

            if processed[columnIdentifier] in replacementDict:
                processed[columnIdentifier] = replacementDict[processed[columnIdentifier]]
            pWriter.writerow(processed)
            sWriter.writerow(source)
    
    os.system("cp " + subfolder + processedPrefix + "%02d" % i + ".tsv" + " " + processedPrefix + "%02d" % i + ".tsv")
    os.system("cp " + subfolder + sourcePrefix + "%02d" % i + ".tsv" + " " + sourcePrefix + "%02d" % i + ".tsv")
    
os.system("rm -r temp")
