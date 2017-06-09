import os
import re
import pandas

queryTypeSubfolder = "queryType/queryTypeFiles/"

duplicatesFile = "duplicates.txt"

processedPrefix = "QueryProcessedOpenRDF"

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
    fileName = processedPrefix + "%02d" % i + ".tsv"
    df = pandas.read_csv(fileName, sep="\t", header=0, index_col=0)
    df[columnIdentifier].replace(replacementDict, inplace=True)
    df.to_csv(fileName, sep="\t")