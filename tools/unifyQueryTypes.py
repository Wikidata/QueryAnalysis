import argparse
import csv
import glob
import gzip
import os
import re
import shutil
import sys

from distutils.dir_util import copy_tree

from utility import utility

import config

# This script uses fdupes to check the queryTypeFiles-Folder and the reference query types folder for duplicates.
# If it finds duplicates it chooses one of the queryTypes (the one in the reference query types folder if one exists, otherwise at random)
# and renames all other duplicate entries in #QueryType to that query type

def unifyQueryTypes(month, monthsFolder = config.monthsFolder, referenceDirectory = config.queryReferenceDirectory, fdupesExecutable = config.fdupesExecutable):
	if os.path.isfile(utility.addMissingSlash(monthsFolder) + utility.addMissingSlash(month) + "locked"):
		print "ERROR: The month " + month + " is being edited at the moment."
		sys.exit()
	else:
		open(utility.addMissingSlash(monthsFolder) + utility.addMissingSlash(month) + "locked", 'a').close()

	owd = os.getcwd()
	os.chdir(utility.addMissingSlash(monthsFolder) + utility.addMissingSlash(month))

	referenceQueryTypeDirectory = utility.addMissingSlash(referenceDirectory)

	readme = "README.md"

	processedFolder = "processedLogData/"

	processedPrefix = "QueryProcessedOpenRDF"
	processedSuffix = ".tsv.gz"

	processedFilePrefix = processedFolder + processedPrefix

	# Variable for the folder containing the query-Type files
	queryTypeSubfolder = processedFolder + "queryTypeFiles/"

	temporaryDirectory = "temp/"

	if not os.path.exists(temporaryDirectory):
		os.makedirs(temporaryDirectory)

	if not os.path.exists(temporaryDirectory + processedFolder):
		os.makedirs(temporaryDirectory + processedFolder)

	duplicatesFile = "duplicates.txt"

	if not os.path.exists(fdupesExecutable):
		print "ERROR: Could not find fdupes executable."
		sys.exit(1)

	if referenceQueryTypeDirectory != None:
		os.system(fdupesExecutable + " " + queryTypeSubfolder + " " + referenceQueryTypeDirectory + " > " + duplicatesFile)
	else:
		os.system(fdupesExecutable + " " + queryTypeSubfolder + " > " + duplicatesFile)

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
			if line.endswith("README.md\n"):
				continue
			if line == "\n":
				if len(block) < 2:
					continue

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
								replacementDictReferenceFolder[toBeReplaced] = referenceQueryType
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
			if (file.endswith(readme)):
				continue
			referenceFiles.add(file)
		break

	for (dirpath, dirnames, filenames) in os.walk(queryTypeSubfolder):
		for file in filenames:
			if (file.endswith(readme)):
				continue
			localFiles.add(file)
		break

	replacementReplacementDict = dict()

	referenceDirectorySize = len(referenceFiles)

	for localFile in localFiles:

		# If there is a naming conflict with the reference folder we add the number of reference directory files to the file name
		if localFile in referenceFiles:
			referenceDirectorySize += 1

			localFileParts = localFile.split(".")
			newName = localFileParts[0] + "_%d" % referenceDirectorySize

			replacementReplacementDict[localFileParts[0]] = newName
			replacementDict[localFileParts[0]] = newName
			os.rename(queryTypeSubfolder + localFileParts[0] + ".queryType", queryTypeSubfolder + newName + ".queryType")

	# Every entry that mapped a query type to a query type that was renamed because of a naming conflict is mapped to the new name instead
	for key, value in replacementDict.iteritems():
		if value in replacementReplacementDict:
			replacementDict[key] = replacementReplacementDict[value]

	# Finally, add all entires mapping local query types to reference query types to the replacementDict
	for key, value in replacementDictReferenceFolder.iteritems():
		if key not in replacementDict:
			replacementDict[key] = value
		else:
			print "ERROR: Query type " + key + " appears both as an old and a new query type. This is probably an embarrassing error on the authors part."
			sys.exit(1)

	if referenceQueryTypeDirectory != None:
		for filename in glob.glob(queryTypeSubfolder + "*.queryType"):
			shutil.copy(filename, referenceQueryTypeDirectory + os.path.basename(filename))
			os.remove(filename)

	shutil.rmtree(queryTypeSubfolder)

	columnIdentifier = "#QueryType"

	for filename in glob.glob(processedFilePrefix + "*" + processedSuffix):
		print "Working on " + filename
		with gzip.open(filename) as p, gzip.open(temporaryDirectory + filename, "w") as temp_p:
			pReader = csv.DictReader(p, delimiter="\t")

			pWriter = csv.DictWriter(temp_p, None, delimiter="\t")

			for processed in pReader:
				if pWriter.fieldnames is None:
					ph = dict((h, h) for h in pReader.fieldnames)
					pWriter.fieldnames = pReader.fieldnames
					pWriter.writerow(ph)

				if processed[columnIdentifier] in replacementDict:
					processed[columnIdentifier] = replacementDict[processed[columnIdentifier]]
				pWriter.writerow(processed)

		shutil.copy(temporaryDirectory + filename, filename)

	shutil.rmtree(temporaryDirectory)

	os.remove(utility.addMissingSlash(monthsFolder) + utility.addMissingSlash(month) + "locked")
	os.chdir(owd)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(
		description="This script uses fdupes to check the queryTypeFiles-Folder and the reference query types folder for duplicates.\n" +
		"If it finds duplicates it chooses one of the queryTypes (the one in the reference query types folder if one exists, otherwise at random)\n" +
		"and renames all other duplicate entries in #QueryType to that query type ")
	parser.add_argument("--monthsFolder", "-m", default=config.monthsFolder, type=str,
	                    help="The folder in which the months directories are residing.")
	parser.add_argument("--referenceDirectory", "-r", default=config.queryReferenceDirectory, type=str,
					help="The directory with the reference query types.")
	parser.add_argument("--fdupesExecutable", "-f", default=config.fdupesExecutable, type=str,
					help="The location of the fdupes executable.")
	parser.add_argument("month", type=str, help="The month whose query types should be unified.")

	if (len(sys.argv[1:]) == 0):
		parser.print_help()
		parser.exit()

	args = parser.parse_args()

	unifyQueryTypes(args.month, args.monthsFolder, args.referenceDirectory, args.fdupesExecutable)
