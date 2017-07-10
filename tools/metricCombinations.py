import argparse
import csv
import os
import sys
from collections import defaultdict

from itertools import izip

from postprocess import processdata
from utility import utility

parser = argparse.ArgumentParser(
	description="This script generates a table that maps how often each metric entry is used in combination with each other.")
parser.add_argument("--monthsFolder", "-m", default="/a/akrausetud/months", type=str,
                    help="The folder in which the months directory are residing.")
parser.add_argument("--outputPath", "-o", type=str, help="The path where the output files should be generated.")
parser.add_argument("metric", type=str, help="The metric that should be matched")
parser.add_argument("month", type=str, help="The month for which the ranking should be generated.")

if (len(sys.argv[1:]) == 0):
	parser.print_help()
	parser.exit()

args = parser.parse_args()

argMetric = args.metric
metric = ""

if argMetric.startswith("#"):
	metric = argMetric[1:]
else:
	metric = argMetric

pathBase = utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(args.month) + utility.addMissingSlash(metric + "_Combinations")

if not os.path.exists(pathBase):
	os.makedirs(pathBase)

# This script generates a table that maps how often each predicate is used in combination with each predicate
# TODO: Command line parameters

def writeOut(filename, fieldValues, dictionary):
	with open(filename, "w") as file:
		header = "hour/combinedWith"
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
	
class combinationsHandler:
	monthlyFieldValues = set()

	monthlyData = dict()
	
	allDailyFieldValues = dict()
	
	allDailyData = dict() 
	
	def handle(self, sparqlQuery, processed):
		if processed['#Valid'] != "VALID":
			return

		keys_string = processed['#' + metric]
		
		if keys_string == "":
			return

		keys_array = keys_string.split(",")
		
		day = processed["#day"]
				
		if day not in self.allDailyFieldValues:
			self.allDailyFieldValues[day] = set()
			self.allDailyData[day] = dict()

		for key in keys_array:
			
			if key not in self.allDailyFieldValues[day]:
				self.allDailyFieldValues[day].add(key)
				self.allDailyData[day][key] = defaultdict(int)
			if key not in self.monthlyFieldValues:
				self.monthlyFieldValues.add(key)
				self.monthlyData[key] = defaultdict(int)
			for match in keys_array:
				if match == key:
					continue
				self.allDailyData[day][key][match] += 1
				self.monthlyData[key][match] += 1
				
	def write(self):
		writeOut(pathBase + "Full_Month_" + metric + "_Combinations.tsv", self.monthlyFieldValues, self.monthlyData)
		for day in self.allDailyFieldValues:
			writeOut(pathBase + "Day_" + str(day) + "_" + metric + "_Combinations.tsv", self.allDailyFieldValues[day], self.allDailyData[day])
			
handler = combinationsHandler()

processdata.processMonth(handler, args.month, args.monthsFolder)

handler.write()
