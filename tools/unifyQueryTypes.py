import csv
import getopt
import os
import re
import shutil
from distutils.dir_util import copy_tree

import sys
from itertools import izip

help = 'Usage: unifyQueryTypes.py -d <directory with processed files> -r <directory with reference query types>'

processedPrefix = "QueryProcessedOpenRDF"
sourcePrefix = "queryCnt"

directory = ""
referenceQueryTypeDirectory = None

# reads the command line arguments -d and -r
try:
	opts, args = getopt.getopt(sys.argv[1:], "hd:r:", ["directory=", "referenceQueryTypes="])
except getopt.GetoptError:
	print help
	sys.exit(2)
for opt, arg in opts:
	if opt == "-h":
		print help
		sys.exit()
	elif opt in ("-d", "--directory"):
		if arg[len(arg) - 1] != "/":
			directory = arg + "/"
		else:
			directory = arg
	elif opt in ("-r", "--referenceQueryTypes"):
		if arg[len(arg) - 1] != "/":
			referenceQueryTypeDirectory = arg + "/"
		else:
			referenceQueryTypeDirectory = arg

if directory == "":
	print "WARNING: No directory specified with -d, assuming the directory this file is in."

if referenceQueryTypeDirectory == None:
	print "WARNING: No directory with/for reference query types given, assuming it is being created now."

# Variable for the folder containing the query-Type files
queryTypeSubfolder = directory + "queryType/queryTypeFiles/"

temporaryDirectory = directory + "temp/"

if not os.path.exists(temporaryDirectory):
	os.makedirs(temporaryDirectory)

duplicatesFile = directory + "duplicates.txt"

if not os.path.exists("fdupes"):
	print "WARNING: Could not find fdupes executable next to this script. Assuming it is installed on this machine."

if referenceQueryTypeDirectory != None:
	os.system("fdupes " + queryTypeSubfolder + " " + referenceQueryTypeDirectory + " > " + duplicatesFile)
else:
	os.system("fdupes " + queryTypeSubfolder + " > " + duplicatesFile)

# Dictionary to hold the replacement query type for all duplicate query types
# (separate because we might need to replace some of these that have the same name as the ones in the reference folder)
replacementDict = dict()

# Dictionary to held the replacement query type for all query types that exist in the reference folder
replacementDictReferenceFolder = dict()

# Since fdupes output consists of the full path and we have to differentiate between duplicates from this month and from the reference folder
# we create a regex to find the query type name from the duplicates.txt-line (one for this month and one for the reference directory)
patternHere = re.compile(queryTypeSubfolder + "(.*)\.queryType\n")
patternReference = None
if referenceQueryTypeDirectory != None:
	patternReference = re.compile(referenceQueryTypeDirectory + "(.*)\.queryType\n")


# takes a list of lines from duplicates.txt (all files point to the same query type) and unifies them by adding an entry to the replacement dict
# for each duplicate query type as well as deleting said duplicate query types
def handleNewQueryTypes(queryTypeBlock):
	unifiyngQueryType = patternHere.match(block.pop(0)).group(1)
	for entry in block:
		os.remove(entry[:-1])
		replacementDict[patternHere.match(entry).group(1)] = unifiyngQueryType


# Reads the lines from fdupes output and separates it into blocks (each terminated by endline character)
with open(duplicatesFile) as dupes:
	block = []

	for line in dupes:
		if line == "\n":

			# If the reference query type directory was not set every block is a new block (meaning there is no query type in the reference folder that might be equal to this block)
			if referenceQueryTypeDirectory == None:
				handleNewQueryTypes(block)
			else:
				referenceQueryType = ""
				entryToDelete = ""

				# Find the line in this block that matches the reference query type directory (if it exists)
				for entry in block:
					match = patternReference.match(entry)
					if match != None:
						referenceQueryType = match.group(1)
						entryToDelete = entry
						break

				if referenceQueryType == "":
					handleNewQueryTypes(block)
				else:
					block.remove(entryToDelete)
					for entry in block:
						match = patternReference.match(entry)
						if match != None:
							print "ERROR: Directory with/for reference query types" + referenceQueryTypeDirectory + "contains duplicates."
							print "Query type " + referenceQueryType + " is the same as " + match.group(1) + "."
							sys.exit(1)
						os.remove(entry[:-1])
						toBeReplaced = patternHere.match(entry).group(1)
						if toBeReplaced != referenceQueryType:
							replacementDictReferenceFolder[patternHere.match(entry).group(1)] = referenceQueryType
			block = []
			continue
		block.append(line)

os.remove(duplicatesFile)

# set of files in the reference directory to check for conflicts with the new query types
referenceFiles = set()
# set of files in the current working directory for query types to check for conflicts with the reference query-Types
localFiles = set()

for (dirpath, dirnames, filenames) in os.walk(referenceQueryTypeDirectory):
	for file in filenames:
		referenceFiles.add(file)
	break

for (dirpath, dirnames, filenames) in os.walk(queryTypeSubfolder):
	for file in filenames:
		localFiles.add(file)
	break

replacementReplacementDict = dict()

for localFile in localFiles:

	# If there is a naming conflict with the reference folder we add an increasing number to the query type until
	if localFile in referenceFiles:
		i = 1
		localFileParts = localFile.split(".")
		newName = localFileParts[0] + "_%d" % i
		while (newName + "." + localFileParts[1] in referenceFiles):
			i += 1
		replacementReplacementDict[localFileParts[0]] = newName
		replacementDict[localFileParts[0]] = newName
		os.rename(queryTypeSubfolder + localFileParts[0] + ".queryType", queryTypeSubfolder + newName + ".queryType")

for key, value in replacementDict.iteritems():
	if value in replacementReplacementDict:
		replacementDict[key] = replacementReplacementDict[value]

for key, value in replacementDictReferenceFolder.iteritems():
	if key not in replacementDict:
		replacementDict[key] = value
	else:
		print "ERROR: Query type " + key + " appears both as an old and a new query type. This is probably an embarrassing error on the authors part."
		sys.exit(1)

if referenceQueryTypeDirectory != None:
	copy_tree(queryTypeSubfolder, referenceQueryTypeDirectory)

columnIdentifier = "#QueryType"

for i in xrange(1, 3):
	print "Working on %02d" % i
	with open(directory + processedPrefix + "%02d" % i + ".tsv") as p, open(
									directory + sourcePrefix + "%02d" % i + ".tsv") as s, open(
								temporaryDirectory + processedPrefix + "%02d" % i + ".tsv", "w") as user_p, open(
								temporaryDirectory + sourcePrefix + "%02d" % i + ".tsv", "w") as user_s:
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

	shutil.copy(temporaryDirectory + processedPrefix + "%02d" % i + ".tsv",
	            directory + processedPrefix + "%02d" % i + ".tsv")
	shutil.copy(temporaryDirectory + sourcePrefix + "%02d" % i + ".tsv", directory + sourcePrefix + "%02d" % i + ".tsv")

shutil.rmtree(directory + "temp")
