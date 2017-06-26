import argparse
import pprint
from collections import defaultdict

import sys

from postprocess import processdata

parser = argparse.ArgumentParser(description="Counts the used tools/bots in the given folder")
parser.add_argument("--monthsFolder", "-m", type=str, help="the folder in which the months directory are residing")
parser.add_argument("month", type=str, help="the month which we're interested in")

if (len(sys.argv[1:]) == 0):
	parser.print_help()
	parser.exit()

args = parser.parse_args()

class CountToolsHandler:
	toolCounter = defaultdict(int)

	def handle(self, sparqlQuery, processed):
		if (processed['#Valid'] == 'VALID'):
			self.toolCounter[processed['#ToolName']] += 1

	def __str__(self):
		return pprint.pformat(sorted(self.toolCounter.iteritems(), key=lambda x: x[1], reverse=True))

handler = CountToolsHandler()

processdata.processMonth(handler, args.month, monthsFolder=args.monthsFolder)

print handler
