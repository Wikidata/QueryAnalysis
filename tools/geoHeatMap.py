import argparse
import os
from pprint import pprint
import sys
import os.path
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
from postprocess import processdata
from utility import utility
import config

parser = argparse.ArgumentParser(
    description="Generaets a heatmap based on geo coordinates")
parser.add_argument("--monthsFolder", "-m", default=config.monthsFolder,
                    type=str, help="the folder in which the months directory "
                    + "are residing")
parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and execute"
                    + " anyways", action="store_true")
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
    # first get all geo coordinates and save them to a file
    handler = GeoCoordinateCollectorHandler()
    processdata.processMonth(handler, args.month, args.monthsFolder)
    handler.saveSetToJson()

else:
    # parse geoCoordinates.tsv and create choropleth map

    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.coastlines()
    ax.stock_img()

    with open('geoCoordinates.tsv', 'r') as file:
        for line in file:
            lat, lon = line.strip('\n').split(" ")
            plt.plot(float(lat), float(lon), color='red',
                     alpha=.3, marker='.', transform=ccrs.PlateCarree())
            # pprint(line.split(" "))

    plt.show()
