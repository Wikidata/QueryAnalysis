from __future__ import division
from SPARQLWrapper import SPARQLWrapper, JSON

import csv
import getopt
import sys

# This script fetches all wikidata properties as well as all property categories (direct subclass of property)
# and checks P31 (instance of) and P79 (subclasss) recursively until it hit one of the categories or the maximum depth

help = 'Usage: fetchPropertyClassification.py -d <directory to create the file in> -m <maximum depth for instance of/subclass tree>'

directory = ""

maximumDepth = 100

try:
    opts, args = getopt.getopt(sys.argv[1:], "hd:m:", ["directory=", "maximumDepth="])
except getopt.GetoptError:
    print help
    sys.exit(2)
for opt, arg in opts:
	if opt == "-h":
		print help
		sys.exit()
	elif opt in ("-d", "--directory"):
		if arg[len(arg) - 1] != "/":
			directory = arg + "/"
		else:
			directory = arg
	elif opt in ("-m", "-maximumDepth"):
		maximumDepth = int(arg)

def cleanupEntity(entity):
	return entity[entity.rfind("/")+1:len(entity)]

toolComment = "#TOOL: arnad\n"

sparql = SPARQLWrapper("https://query.wikidata.org/bigdata/namespace/wdq/sparql", agent="arnad", returnFormat=JSON)
sparql.setQuery(toolComment + "SELECT ?property WHERE {?property a wikibase:Property}")

results = sparql.query().convert()

propertyDict = dict()

for result in results["results"]["bindings"]:
    propertyDict[cleanupEntity(result["property"]["value"])] = set()

sparql.setQuery("#TOOL: arnad\n" + "SELECT ?property WHERE {?property wdt:P279 wd:Q18616576}")

results = sparql.query().convert()

groupsSet = set()

depthExceededSet = set()

intermediateResultDict = dict()

for result in results["results"]["bindings"]:
    groupsSet.add(cleanupEntity(result["property"]["value"]))

def groupsForEntity(entity, depth):
	if depth > maximumDepth:
		depthExceededSet.add(entity)
		return set()

	resultingGroups = set()
    
	resultSet = set()

	sparql.setQuery(toolComment + "SELECT ?instanced WHERE {wd:" + entity + " wdt:P31 ?instanced}")
	results = sparql.query().convert()
    
	for result in results["results"]["bindings"]:
		resultSet.add(cleanupEntity(result["instanced"]["value"]))
    
	sparql.setQuery(toolComment + "SELECT ?subclassed WHERE {wd:" + entity + " wdt:P279 ?subclassed}")
	nextresults = sparql.query().convert()
	
	for result in nextresults["results"]["bindings"]:
		resultSet.add(cleanupEntity(result["subclassed"]["value"]))

	for result in resultSet:
		if result in groupsSet:
			resultingGroups.add(result)
		elif result in intermediateResultDict:
			resultingGroups.update(intermediateResultDict[result])
		else:
			intermediateResultDict[result] = groupsForEntity(result, depth + 1)
			resultingGroups.update(intermediateResultDict[result])
            
	return resultingGroups

i = 0
for key in propertyDict:
	i += 1
	progress = i / len(propertyDict) * 100
	print "Progress: " + str(progress) + " percent"
	propertyDict[key] = groupsForEntity(key, 1)

with open(directory + "propertyGroupMapping.tsv", "w") as target:
	
	typeWriter = csv.DictWriter(target, None, delimiter="\t")
	
	fieldNames = ["property", "listOfGroups"]
	
	typeWriter.fieldnames = fieldNames
	
	th = dict()
	
	for field in fieldNames:
		th[field] = field

	typeWriter.writerow(th)
	
	for key, value in propertyDict.iteritems():
		
		row = dict()
		
		row["property"] = key
		
		valueString = ""
		
		if value != None:
			for entry in value:
				valueString += entry + ","
			valueString = valueString[:-1]
		
		row["listOfGroups"] = valueString
		
		typeWriter.writerow(row)
			
print "Maximum depth exceeded at:"
for entry in depthExceededSet:
	print entry