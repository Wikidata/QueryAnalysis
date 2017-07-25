import argparse
import os
from pprint import pprint
import sys
from collections import defaultdict
import os.path
import sys

from postprocess import processdata
from utility import utility

parser = argparse.ArgumentParser(description="Generaets a heatmap based on geo coordinates")
parser.add_argument("--monthsFolder", "-m", default="/a/akrausetud/months", type=str,
                    help="the folder in which the months directory are residing")
parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and execute anyways", action="store_true")
parser.add_argument("month", type=str, help="the month which we're interested in")

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

if os.path.isfile(utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(args.month) + "locked") and not args.ignoreLock:
    print "ERROR: The month " + args.month + " is being edited at the moment. Use -i if you want to force the execution of this script."
    sys.exit()

# first get all geo coordinates and save them to a file
class GeoCoordinateCollectorHandler:
    coordinates = set()

    def handle(self, sparqlQuery, processed):
        if (processed['#Valid'] == 'VALID' or processed['#Valid'] == '1'):
            if(processed['#Coordinates'] is not ''):
                for coordinate in processed['#Coordinates'].split(","):
                    self.coordinates.add(coordinate)
    
    def saveSetToJson(self):
        pprint(self.coordinates)
        with open('geoCoordinates.tsv', 'w') as geoCoordinatesFile:
            for coordinate in self.coordinates:
                geoCoordinatesFile.write(coordinate.replace(" ", "\t") + "\n")


if not os.path.isfile('geoCoordinates.tsv'):
    handler = GeoCoordinateCollectorHandler()
    processdata.processDay(handler, 19, args.month, args.monthsFolder)
    handler.saveSetToJson()

else:
    # parse geoCoordinates.tsv and create choropleth map
    with open('geoCoordinates.tsv', 'r') as file:
        for line in file:
            pprint(line.split(" "))
