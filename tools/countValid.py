import argparse
from collections import defaultdict

import sys

from postprocess import processdata

parser = argparse.ArgumentParser(description="Counts the valid queries")
parser.add_argument("--monthsFolder", "-m", default="/a/akrausetud/months", type=str,
					help="the folder in which the months directory are residing")
parser.add_argument("month", type=str, help="the month which we're interested in")

if (len(sys.argv[1:]) == 0):
	parser.print_help()
	parser.exit()

args = parser.parse_args()


class CountValidityHandler:
	validCounter = defaultdict(int)

	def handle(self, sparqlQuery, processed):
		self.validCounter[processed['#Valid']] += 1

	def __str__(self):
		if 'VALID' in self.validCounter:
			validCount = self.validCounter['VALID']
			invalidCount = self.validCounter['INVALID']
		else:
			validCount = self.validCounter['1']
			invalidCount = self.validCounter['-1']
		return "Valid: \t\t" + str(validCount) + " " + str(
			float(validCount) / (
				validCount + invalidCount) * 100) + "%" + "\n" + \
			   "Invalid:\t" + str(invalidCount) + " " + str(float(invalidCount) / (
			validCount + invalidCount) * 100) + "%"


handler = CountValidityHandler()

processdata.processMonth(handler, args.month, args.monthsFolder)

print(handler)
