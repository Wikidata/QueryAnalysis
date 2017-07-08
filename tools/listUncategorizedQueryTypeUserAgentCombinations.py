# lists the top 10 queryPatterns with their respective user agents and some example queries which haven't been classified
import argparse
import collections
import csv
import linecache
import os
import sys
import urlparse

from pip._vendor.distlib.util import proceed

from utility import utility
from postprocess import processdata

parser = argparse.ArgumentParser(
	description="This script lists the top N query types with their respective user agents and some example queries.")
parser.add_argument("--monthsFolder", "-m", default="/a/akrausetud/months", type=str,
                    help="The folder in which the months directory are residing.")
parser.add_argument("--outputPath", "-o", type=str, help="The path where the output files should be generated.")
parser.add_argument("--numberOfQueryTypes", "-n", default=10, type=int, help="The top n query types to list.")
parser.add_argument("--queryTypeRanking", "-q", default="Full_Month_QueryType_Ranking.tsv", type=str, help="The file in the QueryType-Folder to use as the ranking.")
parser.add_argument("month", type=str, help="The month for which the ranking should be generated.")

if (len(sys.argv[1:]) == 0):
	parser.print_help()
	parser.exit()

args = parser.parse_args()

monthPath = utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(args.month)
pathBase = monthPath + "userAgentQueryTypeCombinations/"

if not os.path.exists(pathBase):
	os.makedirs(pathBase)

# grep QueryProcessedOpenRDFXX.tsv for these queryTypes and the respective userAgents and create a directory for
# each of these found userAgents and put all found querys in it

class listCombinationsHandler:
	
	queryTypeUserAgentCombinationsCount = dict()
	
	def prepare(self):
		with open(monthPath + "QueryType/" + args.queryTypeRanking) as f:
			for line_no, line in enumerate(csv.reader(f, delimiter="\t")):
				# skip header
				if line_no == 0:
					continue
				if line_no > args.numberOfQueryTypes + 1:
					break
				queryType = line[0]
				self.queryTypeUserAgentCombinationsCount[queryType] = dict()
				self.queryTypeUserAgentCombinationsCount[queryType]['rank'] = line_no
				self.queryTypeUserAgentCombinationsCount[queryType]['userAgent'] = dict()
	
	def handle(self, sparqlQuery, processed):
		if processed["#Valid"] != "VALID":
			return
		
		queryType = processed['#QueryType']
		userAgent = processed['#user_agent']
		toolName = processed['#ToolName']
	
		if queryType in self.queryTypeUserAgentCombinationsCount.keys():
			if userAgent not in self.queryTypeUserAgentCombinationsCount[queryType]['userAgent'].keys():
				self.queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent] = dict()
				self.queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent]['existingToolNames'] = set()
				self.queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent]['count'] = 0
				self.queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent]['queries'] = set()
			else:
				self.queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent]['count'] += 1

			self.queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent]['existingToolNames'].add(toolName)
			self.queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent]['queries'].add(sparqlQuery)
			
	def writeOut(self):
		for queryType, userAgentCountDict in self.queryTypeUserAgentCombinationsCount.iteritems():
			for userAgent, valueDict in userAgentCountDict['userAgent'].iteritems():
				path = pathBase + queryType + "/" + str(valueDict['count']) + "_" + str(userAgent).replace('/', 'SLASH')[:100] + "/"

				if not os.path.exists(path):
					os.makedirs(path)
					# save userAgent etc. in extra file
				with open(path + "info.txt", "w") as info_file:
					
					toWrite = "#UserAgent:\n" + userAgent
					toWrite += "\n#ExistingToolNames: " + ', '.join(self.queryTypeUserAgentCombinationsCount[queryType]['userAgent'][userAgent]['existingToolNames'])
					toWrite += "\n#Rank: " + str(self.queryTypeUserAgentCombinationsCount[queryType]['rank'])
					info_file.write(toWrite)
				
				i = 0
				# save all querys in path
				for query in valueDict['queries']:
					with open(path + str(i) + ".sparql", "w") as text_file:
						text_file.write(query)
						i += 1

				combinations = dict()

				# rank queryTypeUserAgentCombinations
				for queryType, userAgentCountDict in self.queryTypeUserAgentCombinationsCount.iteritems():
					for userAgent, valueDict in userAgentCountDict['userAgent'].iteritems():
						combinations[valueDict['count']] = queryType + "/" + str(valueDict['count']) + "_" + str(userAgent).replace('/','SLASH')[:100]

				sortedCombinations = collections.OrderedDict(sorted(combinations.items(), reverse=True))

				ranking = str()

				for rank, combination in sortedCombinations.iteritems():
					ranking += str(rank) + ":\t\t" + combination + "\n"

				with open(pathBase + "ranking.txt", "w") as ranking_file:
					ranking_file.write(ranking)
					
handler = listCombinationsHandler()

handler.prepare()

processdata.processMonth(handler, args.month, args.monthsFolder)

handler.writeOut()

