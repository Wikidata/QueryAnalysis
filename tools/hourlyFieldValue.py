import argparse
import csv
import os
import sys

from itertools import izip

from postprocess import processdata
from utility import utility

def writeOut(filename, fieldValues, dictionary):
	with open(filename, "w") as file:
		header = "hour"
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

parser = argparse.ArgumentParser(
	description="This script creates tables displaying all values of the specified field and their hourly count.")
parser.add_argument("--monthsFolder", "-m", default="/a/akrausetud/months", type=str,
                    help="The folder in which the months directory are residing.")
parser.add_argument("--outputPath", "-o", type=str, help="The path where the output files should be generated.")
parser.add_argument("metric", type=str, help="The metric that should be ranked")
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

pathBase = utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(args.month) + utility.addMissingSlash(metric)

if not os.path.exists(pathBase):
	os.makedirs(pathBase)

class hourlyFieldValueHandler:
	monthlyFieldValues = set()

	monthlyData = dict()
	
	allDailyFieldValues = dict()
	
	allDailyData = dict()
	
	def handle(self, sparqlQuery, processed):
		if processed['#Valid'] != "VALID":
			return
		
		day = processed["#day"]
		
		if day not in self.allDailyFieldValues:
			self.allDailyFieldValues[day] = set()
			self.allDailyData[day] = dict()
			for j in xrange(0, 24):
				self.allDailyData[day][j] = dict()
				self.monthlyData[j + 24 * (day - 1)] = dict()

		try:
			hour = int(processed["#hour"])
		except ValueError:
			print processed["#hour"] + " could not be parsed as integer"
			return

		if hour in self.allDailyData[day]:
			data = processed["#" + metric]
			self.allDailyFieldValues[day].add(data)
			self.monthlyFieldValues.add(data)
			if data in self.allDailyData[day][hour]:
				self.allDailyData[day][hour][data] += 1
			else:
				self.allDailyData[day][hour][data] = 1

			monthlyHour = hour + 24 * (day - 1)
			if data in self.monthlyData[monthlyHour]:
				self.monthlyData[monthlyHour][data] += 1
			else:
				self.monthlyData[monthlyHour][data] = 1
		else:
			print hour + " is not in 0-23"
			
	def writeHourlyValues(self):
		writeOut(pathBase + "Full_Month_" + metric + "_Hourly_Values.tsv", self.monthlyFieldValues, self.monthlyData)
		for day in self.allDailyFieldValues:
			writeOut(pathBase + "Day_" + str(day) + "_" + metric + "_Hourly_Values.tsv", self.allDailyFieldValues[day], self.allDailyData[day])
			
handler = hourlyFieldValueHandler()

processdata.processMonth(handler, args.month, args.monthsFolder)

handler.writeHourlyValues()
