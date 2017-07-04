"use strict";
var depth = 10;

var sparql = "#Tool: jgonsior-tree \n";
sparql += "SELECT ?property ?propertyLabel ?subclass0 ?subclass0Label";
for (var i = 1; i < depth; i++) {
	sparql += " ?subclass" + i + " ?subclass" + i + "Label ";
}

sparql += "\n WHERE {{ BIND (wd:Q18616576 as ?property) ?subclass0 wdt:P279 ?property .";

for (var i = 1; i < depth; i++) {
	sparql += "\n OPTIONAL {?subclass" + i + " wdt:P279 ?subclass" + (i - 1) + " ."
}

for (var i = 0; i < depth; i++) {
	sparql += "}"
}

sparql += " SERVICE wikibase:label { bd:serviceParam wikibase:language \"en\" }}";
console.log(sparql);
var rootNode = {};
rootNode.name = "/";
rootNode.qid = "/";
rootNode.parentQid = null;
rootNode.parentName = null;
rootNode.children = [];
rootNode.orphans = false;

var ranking;

$.getJSON("ranking.json", function (data) {
	ranking = data;

	$.getJSON('https://query.wikidata.org/sparql', {
		query: sparql
	}).done(function (data) {
		$.each(data.results, function (binding, bindings) {
			$.each(bindings, function (key, value) {

				// @todo: merge this code with the one below?!
				var parent = null;

				// does parent exists already?
				$.each(rootNode.children, function (key, possibleParent) {
					if (possibleParent.qid === value.property.value) {
						parent = possibleParent;
					}
				});

				if (parent === null) {
					parent = {};
					parent.name = value.propertyLabel.value;
					parent.qid = value.property.value;
					parent.parentQid = "/";
					parent.parentName = "/";
					parent.children = [];

					rootNode.children.push(parent);
				}
				for (var i = 0; i < depth; i++) {
					if (value.hasOwnProperty("subclass" + i)) {
						var child = null;

						// does parent exists already?
						$.each(parent.children, function (key, possibleChild) {
							if (possibleChild.qid === value["subclass" + i].value) {
								child = possibleChild;
							}
						});

						if (child === null) {
							child = {};


							child.qid = value["subclass" + i].value;
							if (child.qid in ranking) {
								child.name = value["subclass" + i + "Label"].value + " " + ranking[child.qid][1];
							} else {
								child.name = value["subclass" + i + "Label"].value;
							}
							child.parentQid = parent.qid;
							child.parentName = parent.name;
							child.children = [];

							parent.children.push(child);
						}

						parent = child;
					}
				}
			});

			console.log(rootNode)
		});
	});
});