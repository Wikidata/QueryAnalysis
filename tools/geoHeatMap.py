import argparse
import os
from pprint import pprint
import sys
import os.path
from postprocess import processdata
from utility import utility
import config

parser = argparse.ArgumentParser(description="Generates a heatmap based on "
                                 + "geo coordinates")
parser.add_argument("--monthsFolder", "-m", default=config.monthsFolder,
                    type=str, help="the folder in which the months directory"
                    + " are residing")
parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and "
                    + "execute anyways", action="store_true")
parser.add_argument("month", type=str,
                    help="the month which we're interested in")

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

if os.path.isfile(utility.addMissingSlash(args.monthsFolder)
                  + utility.addMissingSlash(args.month) + "locked") \
   and not args.ignoreLock:
    print "ERROR: The month " + args.month + " is being edited at the moment."
    + " Use -i if you want to force the execution of this script."
    sys.exit()

# user, bot, unknown
# date


class GeoCoordinateCollectorHandler:
    coordinates = set()

    def handle(self, sparqlQuery, processed):
        if (processed['#Valid'] == 'VALID' or processed['#Valid'] == '1'):
            if(processed['#Coordinates'] is not ''):
                for coordinate in processed['#Coordinates'].split(","):
                    self.coordinates.add(coordinate)

    def saveSetToFile(self):
        pprint(self.coordinates)
        with open('geoCoordinates.tsv', 'w') as geoCoordinatesFile:
            for coordinate in self.coordinates:
                geoCoordinatesFile.write(coordinate.replace(" ", "\t") + "\n")


# first get all geo coordinates and save them to a file
handler = GeoCoordinateCollectorHandler()
processdata.processMonth(handler, args.month, args.monthsFolder)
handler.saveSetToFile()
