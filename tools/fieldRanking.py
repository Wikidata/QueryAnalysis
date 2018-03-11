import argparse
import os
import sys
from collections import defaultdict
from postprocess import processdata
from utility import utility
import config

def fieldRanking(month, metric, monthsFolder = config.monthsFolder, ignoreLock = False, outputPath = None, outputFilename = None, filterParams = "", nosplitting = False, writeOut = False, notifications = True):
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

	outputFile = month.strip("/").replace("/", "_") + "_" + metric + "_Ranking.tsv"

	if outputFilename is not None:
		outputFile = outputFilename

	header = metric + "\t" + metric + "_count\n"

	filter = utility.filter()

	filter.setup(filterParams)


	class FieldRankingHandler:
		totalMetricCounts = defaultdict(int)

		def handle(self, sparqlQuery, processed):
			if not filter.checkLine(processed):
				return

			for key in utility.fetchEntries(processed, metric, nosplitting = nosplitting):
				self.totalMetricCounts[key] += 1

		def writeOut(self):
			with open(pathBase + outputFile, "w") as file:
				file.write(header)
				for k, v in sorted(self.totalMetricCounts.iteritems(), key=lambda (k, v): (v, k), reverse=True):
					file.write(str(k) + "\t" + str(v) + "\n")

	handler = FieldRankingHandler()

	processdata.processMonth(handler, month, monthsFolder, notifications = notifications)

	if writeOut:
		if not os.path.exists(pathBase):
			os.makedirs(pathBase)
		handler.writeOut()
	return handler.totalMetricCounts

if __name__ == '__main__':
	parser = argparse.ArgumentParser(
		description="This script creates descending rankings for each day for all"
		+ "metrics given.")
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
	parser.add_argument("--nosplitting", "-n", help="Check if you do not want the"
		                + " script to split entries at commas and count each part"
		                + " separately but instead just to sort such entries and "
		                + "count them as a whole.", action="store_true")
	parser.add_argument("metric", type=str,
		                help="The metric that should be ranked")
	parser.add_argument("month", type=str,
		                help="The month for which the ranking should be "
		                +"generated.")


	if (len(sys.argv[1:]) == 0):
		parser.print_help()
		parser.exit()

	args = parser.parse_args()

	fieldRanking(args.month, args.metric, monthsFolder = args.monthsFolder, ignoreLock = args.ignoreLock, outputPath = args.outputPath, outputFilename = args.outputFilename, filterParams = args.filter, nosplitting = args.nosplitting, writeOut = True, notifications = not args.suppressNotifications)
