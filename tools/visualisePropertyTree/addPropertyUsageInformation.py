import argparse
import csv
import json

parser = argparse.ArgumentParser(
	description="Generates based on the content of the ranking.tsv file a JavaScript Json Object which contains detailed usage information about each property")
parser.add_argument("--rankingFile", "-r", default="ranking.tsv", type=str,
					help="the file which contains the ranking information")

args = parser.parse_args()

rankings = dict()

print "Working on: " + args.rankingFile
with open(args.rankingFile, "r") as rankingFile:
	rankingReader = csv.DictReader(rankingFile, delimiter="\t")

	for ranking in rankingReader:
		rankings["http://www.wikidata.org/entity/" + ranking["Categories"]] = (
		ranking["Categories_count"], ranking["percentage"])

with open("ranking.json", "w") as rankingOutputFile:
	json.dump(rankings, rankingOutputFile)
