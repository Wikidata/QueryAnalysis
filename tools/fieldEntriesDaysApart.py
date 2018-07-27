import argparse
import os
import sys
from collections import defaultdict
from dateutil import parser as dateparser
from postprocess import processdata
from utility import utility
import config

def fieldEntriesDaysApart(months, metric, days, monthsFolder = config.monthsFolder, ignoreLock = False, outputPath = None, outputFilename = None, filterParams = "", nosplitting = False, writeOut = False, notifications = True, anonymous = False):
    for month in months.split(","):
        if os.path.isfile(utility.addMissingSlash(monthsFolder) + utility.addMissingSlash(month) + "locked") and not ignoreLock:
            print "ERROR: The month " + month + " is being edited at the moment." + " Use -i or ignoreLock = True if you want to force the execution of this script."
            sys.exit()

    metric = utility.argMetric(metric)

    pathBase = utility.addMissingSlash(monthsFolder) \
		    + utility.addMissingSlash(months) \
		    + utility.addMissingSlash(metric)

    if outputPath is not None:
        pathBase = utility.addMissingSlash(outputPath)

    addString = ""
    if anonymous:
        addString = "_anonymous_"

    outputFile = month.strip("/").replace("/", "_") + "_" + metric + addString + "_" + str(days) + "_days_apart.tsv"

    if outputFilename is not None:
        outputFile = outputFilename

    header = metric + "\n"

    filter = utility.filter()

    filter.setup(filterParams)

    faultyTimestamps = defaultdict(int)

    class FieldEntriesDaysApartHandler:
        firstSeen = dict()
        lastSeen = dict()

        fieldEntries = set()

        def handle(self, sparqlQuery, processed):
            if not filter.checkLine(processed):
                return

            for key in utility.fetchEntries(processed, metric, nosplitting = nosplitting):
                timestamp = processed["#timestamp"]
                try:
                    parsedTime = dateparser.parse(timestamp)
                except ValueError:
                    print "ERROR: Faulty timestamp " + str(timestamp)
                    faultyTimestamps[timestamp] += 1
                    continue
                if not key in self.firstSeen:
                    self.firstSeen[key] = parsedTime
                    self.lastSeen[key] = parsedTime
                if parsedTime > self.lastSeen[key]:
                    self.lastSeen[key] = parsedTime

        def compute(self):
            for key, firstTS in self.firstSeen.iteritems():
                lastTS = self.lastSeen[key]
                if (lastTS - firstTS).days >= days:
                    self.fieldEntries.add(key)

        def writeOut(self):
            with open(pathBase + outputFile, "w") as file:
                file.write(header)
                for key in self.fieldEntries:
                    file.write(str(key) + "\n")

    handler = FieldEntriesDaysApartHandler()

    for month in months.split(","):
        if anonymous:
            processdata.processMonth(handler, month, monthsFolder, anonymous = True, notifications = notifications)
        else:
            processdata.processMonth(handler, month, monthsFolder, notifications = notifications)

    handler.compute()

    if len(faultyTimestamps) > 0:
        print "Faulty timestamp\tcount"
        for k, v in sorted(faultyTimestamps.iteritems(), key=lambda (k, v): (v, k), reverse=True):
            print str(k) + "\t" + str(v)

    if writeOut:
        if not os.path.exists(pathBase):
            os.makedirs(pathBase)
        handler.writeOut()
    return handler.fieldEntries

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
		description="This script creates a list of all entries in a metric that are at least N days apart.")
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
    parser.add_argument("--anonymous", "-a", action="store_true", help="Check to switch to ranking the anonymous data."
            			+ " WARNING: No processed metrics are available for anonymous data because the anonymous files"
                    	+ " do not synch up to the processed files due to dropping the invalid lines.")
    parser.add_argument("metric", type=str,
		                help="The metric that should be ranked")
    parser.add_argument("months", type=str,
		                help="The months for which the ranking should be "
		                +"generated.")
    parser.add_argument("days", type=int, help="How many days should be between the entries.")


    if (len(sys.argv[1:]) == 0):
        parser.print_help()
        parser.exit()

    args = parser.parse_args()
    fieldEntriesDaysApart(args.months, args.metric, args.days, monthsFolder = args.monthsFolder, ignoreLock = args.ignoreLock, outputPath = args.outputPath, outputFilename = args.outputFilename, filterParams = args.filter, nosplitting = args.nosplitting, writeOut = True, notifications = not args.suppressNotifications, anonymous = args.anonymous)
