import json
import os.path

import requests
import urllib.request

with urllib.request.urlopen("https://tools.wmflabs.org/hay/propbrowse/props."
									+ "json") as url:
	allProperties = json.loads(url.read().decode())

	categories = requests.get('https://query.wikidata.org/bigdata/namespace'
							  + '/wdq/sparql', params={
		'query': '#TOOL: jgonsior \n'
				 'SELECT ?item '
				 '?itemLabel WHERE {'
				 '?item wdt:P279 wd:Q18616576 . '
				 'SERVICE wikibase:label '
				 '{bd:serviceParam wikibase:language "en"'
				 '}}',
		'format': 'json'
	}).json()
	print(len(allProperties))

	for category in categories['results']['bindings']:
		for property in allProperties:
			answer = requests.get(
				'https://query.wikidata.org/bigdata/namespace'
				'/wdq/sparql',
				params={
					'query': '#TOOL: jgonsior \n'
							 'SELECT ?x '
							 '{?x wdt:P31 wd:' +
							 os.path.basename(category['item']['value']) + '}',
					'format': 'json'}).json()
		# pprint(answer)
