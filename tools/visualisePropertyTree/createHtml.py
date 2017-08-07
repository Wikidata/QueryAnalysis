import argparse
import csv
from functools import cmp_to_key

from SPARQLWrapper import SPARQLWrapper, JSON

parser = argparse.ArgumentParser(
	description="Generates based on the content of the ranking.tsv file a JavaScript Json Object which contains detailed usage information about each property")
parser.add_argument("--rankingFile", "-r", default="ranking.tsv", type=str,
					help="the file which contains the ranking information")

args = parser.parse_args()

rankings = dict()

print("Working on: " + args.rankingFile)
with open(args.rankingFile, "r") as rankingFile:
	rankingReader = csv.DictReader(rankingFile, delimiter="\t")

	for ranking in rankingReader:
		rankings["http://www.wikidata.org/entity/" + ranking["Categories"]] = (
			int(ranking["Categories_count"]), float(ranking["percentage"]))

# pprint(rankings)

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

sparql.setQuery("""
#Tool: jgonsior-tree 
SELECT ?property ?propertyLabel ?subclass0 ?subclass0Label ?subclass1 ?subclass1Label  ?subclass2 ?subclass2Label  ?subclass3 ?subclass3Label  ?subclass4 ?subclass4Label  ?subclass5 ?subclass5Label  ?subclass6 ?subclass6Label  ?subclass7 ?subclass7Label  ?subclass8 ?subclass8Label  ?subclass9 ?subclass9Label 
 WHERE {{ BIND (wd:Q18616576 as ?property) ?subclass0 wdt:P279 ?property .
 OPTIONAL {?subclass1 wdt:P279 ?subclass0 .
 OPTIONAL {?subclass2 wdt:P279 ?subclass1 .
 OPTIONAL {?subclass3 wdt:P279 ?subclass2 .
 OPTIONAL {?subclass4 wdt:P279 ?subclass3 .
 OPTIONAL {?subclass5 wdt:P279 ?subclass4 .
 OPTIONAL {?subclass6 wdt:P279 ?subclass5 .
 OPTIONAL {?subclass7 wdt:P279 ?subclass6 .
 OPTIONAL {?subclass8 wdt:P279 ?subclass7 .
 OPTIONAL {?subclass9 wdt:P279 ?subclass8 .}}}}}}}}}} SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }}
""")

sparql.setReturnFormat(JSON)
sparqlResult = sparql.query().convert()


def createProperty(name, qid):
	property = dict()
	property['name'] = name
	property['qid'] = qid
	if qid in rankings:
		property['countUserQueries'] = rankings[qid][0]
	else:
		property['countUserQueries'] = 0

	property['children'] = list()
	return property


def searchPropertyInTree(treeRoot, qid):
	for possibleProperty in treeRoot['children']:
		if possibleProperty['qid'] == qid:
			return possibleProperty
	return None


rootProperty = createProperty("/", "/")

for property in sparqlResult['results']['bindings']:
	parentProperty = searchPropertyInTree(rootProperty, property['property']['value'])
	if parentProperty is None:
		parentProperty = createProperty(property['propertyLabel']['value'], property['property']['value'])
		rootProperty['children'].append(parentProperty)

	for i in range(0, 9):
		if 'subclass' + str(i) in property:
			childProperty = searchPropertyInTree(parentProperty, property['subclass' + str(i)]['value'])
			if childProperty is None:
				childProperty = createProperty(property['subclass' + str(i) + "Label"]['value'],
											   property['subclass' + str(i)]['value'])
				parentProperty['children'].append(childProperty)
			parentProperty = childProperty
		else:
			break


def compareProperties(property1, property2):
	if property1['countUserQueries'] < property2['countUserQueries']:
		return -1
	elif property1['countUserQueries'] > property2['countUserQueries']:
		return 1
	else:
		return 0


html = """
<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="utf-8">

	
	<link rel="stylesheet" href="bower_components/jquery-treetable/css/jquery.treetable.css" />
	<link rel="stylesheet" href="bower_components/jquery-treetable/css/jquery.treetable.theme.default.css" />
	<link rel="stylesheet" href="bower_components/bootstrap/dist/css/bootstrap.css" />
	<link rel="stylesheet" href="bower_components/bootstrap/dist/css/bootstrap-theme.css" />
	<link rel="stylesheet" type="text/css" href="style.css">
</head>
<body>
<table id="propertyTreeTable" class="table">
<thead>
<tr>
<th class="property" >Property label</th>
<th class="qid">QID</th>
<th class="percentage">User Queries which had this query</th>
</tr>
</thead>
<tbody>
"""

def createTr(property, parent, parentPrefix):
	html = ""
	parentPrefix += parent['qid']
	html += "<tr data-tt-id=\"" + parentPrefix + property['qid'] + "\" data-tt-parent-id=\"" + parentPrefix + "\">"
	html += "<td class=\"property\">" + property['name'] + "</td>"
	html += "<td class=\"qid\" >" + property['qid'][31:] + "</td>"
	html += "<td class=\"percentage\">" + str(property['countUserQueries']) + "</td>"

	html += "</tr>\n"
	for child in sorted(property['children'], reverse=True, key=cmp_to_key(compareProperties)):
		html += createTr(child, property, parentPrefix)
	return html


html += createTr(rootProperty['children'][0], rootProperty, "")
html += """
</tbody>
</table>
<script src="bower_components/jquery/dist/jquery.js"></script>
<script src="bower_components/jquery-treetable/jquery.treetable.js"></script>
<script type="text/javascript" src="script.js"></script>
</body>
</html>
"""

with open("index.html", "w") as htmlFile:
	htmlFile.write(html)
