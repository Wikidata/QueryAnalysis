import argparse
import os
import sys

from collections import defaultdict

import config

from postprocess import processdata
from utility import utility

def writeOutMethod(filename, fieldValues, dictionary, headerStart):
    with open(filename, "w") as file:
        header = headerStart
        for field in sorted(fieldValues):
            header += "\t" + field
        file.write(header + "\n")
        for j in sorted(dictionary.keys()):
            line = str(j)
            for field in sorted(fieldValues):
                if field in dictionary[j].keys():
                    line += "\t" + str(dictionary[j][field])
                else:
                    line += "\t0"
            file.write(line + "\n")



def xyMapping(month, metricOne, metricTwo, monthsFolder = config.monthsFolder, ignoreLock = False, outputPath = None, outputFilename = None, filterParams = "", nosplittingOne = False, nosplittingTwo = False, writeOut = False):
    if os.path.isfile(utility.addMissingSlash(monthsFolder)
                      + utility.addMissingSlash(month) + "locked") \
       and not ignoreLock:
        print "ERROR: The month " + month + " is being edited at the "
        + "moment. Use -i if you want to force the execution of this script."
        sys.exit()

    metricOne = utility.argMetric(metricOne)
    metricTwo = utility.argMetric(metricTwo)

    folderName = metricOne + "_" + metricTwo

    pathBase = utility.addMissingSlash(monthsFolder) \
            + utility.addMissingSlash(month) \
            + utility.addMissingSlash(folderName)

    outputFile = month.strip("/").replace("/", "_") + "_" + folderName + ".tsv"

    if outputFilename is not None:
    	outputFile = outputFilename

    filter = utility.filter()

    filter.setup(filterParams)


    class hourlyFieldValueHandler:
        monthlyFieldValues = set()

        monthlyData = dict()

        def handle(self, sparqlQuery, processed):
            if not filter.checkLine(processed):
                return

            entriesOne = utility.fetchEntries(processed, metricOne, nosplittingOne)

            for keyTwo in utility.fetchEntries(processed, metricTwo, nosplittingTwo):
                if keyTwo not in self.monthlyData:
                    self.monthlyData[keyTwo] = defaultdict(int)

                for keyOne in entriesOne:
                    self.monthlyFieldValues.add(keyOne)
                    self.monthlyData[keyTwo][keyOne] += 1

        def writeHourlyValues(self):
            writeOutMethod(pathBase + outputFile, self.monthlyFieldValues, self.monthlyData, metricTwo + "\\" + metricOne)

    handler = hourlyFieldValueHandler()

    processdata.processMonth(handler, month, monthsFolder)

    if writeOut:
        if not os.path.exists(pathBase):
            os.makedirs(pathBase)
        handler.writeHourlyValues()
    return (handler.monthlyFieldValues, handler.monthlyData)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="This script creates tables displaying all values of the "
        + "specified first metric and their occurence for the specified second metric count.")
    parser.add_argument("--monthsFolder", "-m", default=config.monthsFolder,
                        type=str, help="The folder in which the months directory "
                        + "are residing.")
    parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and execute"
                        + " anyways", action="store_true")
    parser.add_argument("--outputPath", "-p", type=str, help="The path where the"
                        + " output files should be generated.")
    parser.add_argument("--outputFilename", "-o", type=str, help="The name of the output file to be generated.")
    parser.add_argument("--filter", "-f", default="", type=str,
                        help="Constraints used to limit the lines used to generate"
                        + " the output."
                        + " Default filter is Valid=^VALID$."
                        + " Enter as <metric>=<regex>,<othermetric>/<regex> "
                        + "(e.g. QueryType=wikidataLastModified,ToolName=^USER$)"
                        + " NOTE: If you use this option you should probably also"
                        + "set the --outputPath to some value other than the "
                        + "default.")
    parser.add_argument("--nosplittingOne", "-n1", help="Check if you do not want the"
		                + " script to split entries for metric one at commas and count each part"
		                + " separately but instead just to sort such entries and "
		                + "count them as a whole.", action="store_true")
    parser.add_argument("--nosplittingTwo", "-n2", help="Check if you do not want the"
		                + " script to split entries for metric one at commas and count each part"
		                + " separately but instead just to sort such entries and "
		                + "count them as a whole.", action="store_true")
    parser.add_argument("metricOne", type=str, help="The metric that should be ranked")
    parser.add_argument("metricTwo", type=str, help="The metric that should be ranked")
    parser.add_argument("month", type=str,
                        help="The month for which the ranking should be "
                        + "generated.")

    if (len(sys.argv[1:]) == 0):
        parser.print_help()
        parser.exit()

    args = parser.parse_args()


    xyMapping(args.month, args.metricOne, args.metricTwo, monthsFolder = args.monthsFolder, ignoreLock = args.ignoreLock, outputPath = args.outputPath, outputFilename = args.outputFilename, filterParams = args.filter, nosplittingOne = args.nosplittingOne, nosplittingTwo = args.nosplittingTwo, writeOut = True)
