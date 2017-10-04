import argparse
import os
import sys
from collections import defaultdict
from postprocess import processdata
from utility import utility
import config

# This script creates descending rankings for each day for all metrics (in the
# array metrics)

# This list contains all fields that should not be split because they could contain commas
notToSplit = ["user_agent", "ToolName"]

def fieldRanking(month, metric, monthsFolder = config.monthsFolder, ignoreLock = False, outputPath = None, filterParams = "", nosplitting = False, writeOut = False):
	if os.path.isfile(utility.addMissingSlash(monthsFolder)
		              + utility.addMissingSlash(month) + "locked") \
	   and not ignoreLock:
		print "ERROR: The month " + month + " is being edited at the moment."
		+ " Use -i or ignoreLock = True if you want to force the execution of this script."
		sys.exit()

	argMetric = metric
	metric = ""

	if argMetric.startswith("#"):
		metric = argMetric[1:]
	else:
		metric = argMetric

	pathBase = utility.addMissingSlash(monthsFolder) \
		    + utility.addMissingSlash(month) \
		    + utility.addMissingSlash(metric) + "ranking/"

	if outputPath is not None:
		pathBase = outputPath

	header = metric + "\t" + metric + "_count\tpercentage\n"

	filter = utility.filter()

	filter.setup(filterParams)


	class FieldRankingHandler:
		totalCount = 0
		totalMetricCounts = defaultdict(int)

		dailyCount = defaultdict(int)
		dailyMetricCount = dict()

		def handle(self, sparqlQuery, processed):
		    if not filter.checkLine(processed):
		        return
		       
		    day = processed["#day"]
		    entry = str(processed["#" + metric])
		    
		    if metric in notToSplit:
		    	self.countQuery(day)
		    	self.countEntry(entry, day)
		    else:
		    	field_array = entry.split(",")
		    	if nosplitting:
		    		field_array = sorted(field_array)
		        	self.countQuery(day)
		        	self.countEntry(utility.listToString(field_array), day)
		    	else:
		        	self.countQuery(day)
		        	for entry in field_array:
		        		self.countEntry(entry, day)

		def countEntry(self, entry, day):
		    if (day not in self.dailyMetricCount):
		        self.dailyMetricCount[day] = defaultdict(int)
		    self.totalMetricCounts[entry] += 1
		    self.dailyMetricCount[day][entry] += 1

		def countQuery(self, day):
		    if (day not in self.dailyMetricCount):
		        self.dailyMetricCount[day] = defaultdict(int)
		    self.totalCount += 1
		    self.dailyCount[day] += 1

		def writeOut(self):
		    self.writeCount(pathBase + "Full_Month_" + metric + "_Ranking.tsv",
		                    self.totalMetricCounts, self.totalCount)
		    for day in self.dailyCount:
		        self.writeCount(pathBase + "Day_" + str(day) + "_" + metric
		                        + "_Ranking.tsv", self.dailyMetricCount[day],
		                        self.dailyCount[day])

		def writeCount(self, filename, metricsCount, count):
		    with open(filename, "w") as file:
		        file.write(header)
		        for k, v in sorted(metricsCount.iteritems(),
		                           key=lambda (k, v): (v, k), reverse=True):
		            percentage = float(v) / count * 100
		            file.write(str(k) + "\t" + str(v) + "\t"
		                       + str(percentage) + "\n")

	handler = FieldRankingHandler()

	processdata.processMonth(handler, month, monthsFolder)
	
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
	parser.add_argument("--outputPath", "-o", type=str, help="The path where the "
		                + "output files should be generated.")
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

	fieldRanking(args.month, args.metric, monthsFolder = args.monthsFolder, ignoreLock = args.ignoreLock, outputPath = args.outputPath, filterParams = args.filter, nosplitting = args.nosplitting, writeOut = True)
