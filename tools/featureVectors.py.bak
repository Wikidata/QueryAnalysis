import argparse
import os
import sys
from collections import defaultdict
from postprocess import processdata
from utility import utility

import config
import fieldRanking

def featureVectors(month, metric, monthsFolder = config.monthsFolder, threshold = 100, ignoreLock = False, outputPath = None, outputFilename = None, filterParams = "", writeOut = False, notifications = True):
	if os.path.isfile(utility.addMissingSlash(monthsFolder)
		              + utility.addMissingSlash(month) + "locked") \
	   and not ignoreLock:
		print "ERROR: The month " + month + " is being edited at the moment."
		+ " Use -i or ignoreLock = True if you want to force the execution of this script."
		sys.exit()

	metric = utility.argMetric(metric)

	pathBase = utility.addMissingSlash(monthsFolder) \
		    + utility.addMissingSlash(month) \
		    + utility.addMissingSlash(metric)

	if outputPath is not None:
		pathBase = utility.addMissingSlash(outputPath)

	outputFile = month.strip("/").replace("/", "_") + "_" + metric + "_feature_vectors.tsv"

	if outputFilename is not None:
		outputFile = outputFilename

	filter = utility.filter()

	filter.setup(filterParams)

	vectors = list()

	vectorEntries = set()

	result = fieldRanking.fieldRanking(month, metric, monthsFolder, ignoreLock = ignoreLock, outputPath = outputPath, outputFilename = outputFilename, filterParams = filterParams, nosplitting = True, writeOut = False, notifications = notifications)
	for keyOneEntry, keyOneEntryCount in sorted(result.iteritems(), key = lambda (k, v): (v, k), reverse = True):
		if keyOneEntryCount < threshold:
			break

		if metric not in utility.notToSplit:
			entries = utility.splitEntry(keyOneEntry)
		else:
			entries = [keyOneEntry]

		newVector = defaultdict(int)

		for entry in entries:
			newVector[entry] = 1
			vectorEntries.add(entry)

		newVector["count"] = keyOneEntryCount

		vectors.append(newVector)

	if writeOut:
		if not os.path.exists(pathBase):
			os.makedirs(pathBase)
		with open(pathBase + outputFile, "w") as file:
			headerEntries = sorted(vectorEntries)
			for headerEntry in headerEntries:
				file.write(str(headerEntry) + "\t")
			file.write("count\n")
			for vector in sorted(vectors, key=lambda entry: entry["count"], reverse=True):
				for headerEntry in headerEntries:
					file.write(str(vector[headerEntry]) + "\t")
				file.write(str(vector['count']) + "\n")
	return vectors

if __name__ == '__main__':
	parser = argparse.ArgumentParser(
		description="This script creates a table with columns for all features vectors that occured more than threshold times.")
	parser.add_argument("--monthsFolder", "-m", default=config.monthsFolder,
		                type=str, help="The folder in which the months directory "
		                + "are residing.")
	parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and execute"
		                + " anyways", action="store_true")
	parser.add_argument("--suppressNotifications", "-s", help = "Suppress notifications from processdata.py.", action = "store_true")
	parser.add_argument("--outputPath", "-p", type=str, help="The path where the "
		                + "output file should be generated.")
	parser.add_argument("--outputFilename", "-o", type=str, help="The name of the output file to be generated.")
	parser.add_argument("--filter", "-f", default="", type=str, help="Constraints "
		                + "used to limit the lines used to generate the output."
		                + " Default filter is Valid=^VALID$."
		                + " Enter as <metric>=<regex>,<othermetric>/<regex> (e.g."
		                + " QueryType=wikidataLastModified,ToolName=^USER$)"
		                + " NOTE: If you use this option you should probably also"
		                + " set the --outputPath to some value other than the "
		                + "default.")
	parser.add_argument("--threshold", "-t", default = 100, type = int, help = "The threshold above which the entries should be counted. Default is 100.")
	parser.add_argument("metric", type=str,
		                help="The metric that should be analysed")
	parser.add_argument("month", type=str,
		                help="The month for which the feature vectors should be generated.")


	if (len(sys.argv[1:]) == 0):
		parser.print_help()
		parser.exit()

	args = parser.parse_args()

	featureVectors(args.month, args.metric, monthsFolder = args.monthsFolder, threshold = args.threshold, ignoreLock = args.ignoreLock, outputPath = args.outputPath, outputFilename = args.outputFilename, filterParams = args.filter, writeOut = True, notifications = not args.suppressNotifications)
