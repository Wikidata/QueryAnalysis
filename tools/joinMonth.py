import argparse
import gzip
import os
import sys

import config
from utility import utility

def joinMonth(month, monthsFolder = config.monthsFolder, ignoreLock = False, outputPath = None, outputFilename = None):
    if os.path.isfile(utility.addMissingSlash(monthsFolder) + utility.addMissingSlash(month) + "locked") and not ignoreLock:
        print "ERROR: The month " + month + " is being edited at the moment. Use -i or ignoreLock = True if you want to force the execution of this script."
        sys.exit()

    anonymizedFolder = "anonymousRawData/"
    anonymizedPrefix = anonymizedFolder + "AnonymousQueryCnt"

    pathBase = utility.addMissingSlash(monthsFolder) \
		    + utility.addMissingSlash(month)

    outputFile = month.strip("/").replace("/", "_") + "_Joined.tsv.gz"

    if outputFilename is not None:
        outputFile = outputFilename

    targetFile = pathBase + anonymizedFolder
    if outputPath is not None:
        targetFile = outputPath
    if not os.path.exists(targetFile):
        os.makedirs(targetFile)
    targetFile += outputFile

    with gzip.open(targetFile, "w") as target:
        headerSet = False
        for i in xrange(1, 32):
            sourceFile = pathBase + anonymizedPrefix + "%02d" % i + ".tsv.gz"
            if not (os.path.exists(sourceFile)):
                continue
            with gzip.open(sourceFile) as source:
                if headerSet:
                    next(source)
                else:
                    headerSet = True
                for line in source:
                    target.write(line)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
		description="This script joins all the anonymized files into one while keeping only the first header.")
    parser.add_argument("--monthsFolder", "-m", default=config.monthsFolder,
		                type=str, help="The folder in which the months directory "
		                + "are residing.")
    parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and execute"
		                + " anyways", action="store_true")
    parser.add_argument("--outputPath", "-p", type=str, help="The path where the "
		                + "output file should be generated.")
    parser.add_argument("--outputFilename", "-o", type=str, help="The name of the output file to be generated.")
    parser.add_argument("month", type=str,
                        help="the month which we're interested in")

    if (len(sys.argv[1:]) == 0):
        parser.print_help()
        parser.exit()

    args = parser.parse_args()

    joinMonth(args.month, monthsFolder = args.monthsFolder, ignoreLock = args.ignoreLock, outputPath = args.outputPath, outputFilename = args.outputFilename)
