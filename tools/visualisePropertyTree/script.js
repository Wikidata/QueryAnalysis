"use strict";

var sparql = 'SELECT ?property ?propertyLabel ?subclass1 ?subclass1Label ?subclass2 ?subclass2Label ?subclass3 ?subclass3Label ?subclass4 ?subclass4Label ?subclass5 ?subclass5Label WHERE { {BIND (wd:Q18616576 as ?property) ?subclass1 wdt:P279 ?property . OPTIONAL {?subclass2 wdt:P279 ?subclass1 . OPTIONAL {?subclass3 wdt:P279 ?subclass2 . OPTIONAL {?subclass4 wdt:P279 ?subclass3 . OPTIONAL {?subclass5 wdt:P279 ?subclass4}}}}} SERVICE wikibase:label { bd:serviceParam wikibase:language "en"; }}';

var tree = {
	name: "/",
	contents: []
};

$.get('https://query.wikidata.org/sparql', {
	query: sparql,
	format: 'json'
}, function (data) {
	$.each(data.results, function (binding, bindings) {
		$.each(bindings, function (key, value) {
			var element = {};
			element.name = value.propertyLabel.value;
			element.contents = [];
			tree.contents.push(element);
		});
	});
});

console.log(tree);
var tree = d3.tree();
