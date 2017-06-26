import argparse
from collections import defaultdict

import sys

from postprocess import processdata

parser = argparse.ArgumentParser(description="Counts the valid queries")
parser.add_argument("--monthsFolder", "-m", default="/a/akrausetud/month", type=str,
                    help="the folder in which the months directory are residing")
parser.add_argument("month", type=str, help="the month which we're interested in")

if (len(sys.argv[1:]) == 0):
	parser.print_help()
	parser.exit()

args = parser.parse_args()


class CountValdHandler:
	validCounter = defaultdict(int)

	def handle(self, sparqlQuery, processed):
		self.validCounter[processed['#Valid']] += 1

	def __str__(self):
		return "Valid: \t\t" + str(self.validCounter['VALID']) + " " + str(
			float(self.validCounter['VALID']) / (
				self.validCounter['VALID'] + self.validCounter['INVALID']) * 100) + "%" + "Invalid:\t" + str(
			self.validCounter['INVALID']) + " " + str(float(self.validCounter['INVALID']) / (
			self.validCounter['VALID'] + self.validCounter['INVALID']) * 100) + "%"


handler = CountValdHandler()

processdata.processMonth(handler, args.month, monthsFolder=args.monthsFolder)

print(handler)
