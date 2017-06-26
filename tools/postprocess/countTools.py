import getopt

import sys

import processdata as processdata

help = 'Usage: countTools.py -f <folder>'
folder = ""
folderGiven = False

try:
	opts, args = getopt.getopt(sys.argv[1:], "f", ["folder="])
except getopt.GetoptError:
	print help
	sys.exit(2)
for opt, arg in opts:
	if opt == "-h":
		print help
		sys.exit()
	elif opt in ("-f", "--folder"):
		folder = arg
		folder_given = True

if not (folderGiven):
	print help
	sys.exit()


class CountToolsHandler:
	metrics = ["#QuerySize", "#TripleCountWithService"]

	def handle(self, sparqlQuery, processed):
		pass


handler = CountToolsHandler()

processdata.processFolder(folder, handler)
