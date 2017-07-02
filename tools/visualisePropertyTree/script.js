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

var rootNode = {};
rootNode.name = "/";
rootNode.qid = "/";
rootNode.parentQid = null;
rootNode.parentName = null;
rootNode.children = [];
rootNode.orphans = false;

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
					child.name = value["subclass" + i + "Label"].value;
					child.qid = value["subclass" + i].value;
					child.parentQid = parent.qid;
					child.parentName = parent.name;
					child.children = [];

						parent.children.push(child);
					}

					parent = child;
				}
			}
		});
	});

	console.log(rootNode);

// ************** Generate the tree diagram	 *****************
	var margin = {top: 20, right: 20, bottom: 20, left: 180},
		width = 4000 - margin.right - margin.left,
		height = 5000 - margin.top - margin.bottom;

	var i = 0,
		duration = 750,
		root;

	var tree = d3.layout.tree()
		.size([height, width]);

	var diagonal = d3.svg.diagonal()
		.projection(function (d) {
			return [d.y, d.x];
		});

	var svg = d3.select("body").append("svg")
		.attr("width", width + margin.right + margin.left)
		.attr("height", height + margin.top + margin.bottom)
		.append("g")
		.attr("transform", "translate(" + margin.left + "," + margin.top + ")");

	root = rootNode.children[0];
	root.x0 = height / 2;
	root.y0 = 0;

	update(root);

	root.children.forEach(toggleAll);

	d3.select(self.frameElement).style("height", "500000px");

	function update(source) {

		// Compute the new tree layout.
		var nodes = tree.nodes(root).reverse(),
			links = tree.links(nodes);

		// Normalize for fixed-depth.
		nodes.forEach(function (d) {
			d.y = d.depth * 480;
		});

		// Update the nodes…
		var node = svg.selectAll("g.node")
			.data(nodes, function (d) {
				return d.id || (d.id = ++i);
			});

		// Enter any new nodes at the parent's previous position.
		var nodeEnter = node.enter().append("g")
			.attr("class", "node")
			.attr("transform", function (d) {
				return "translate(" + source.y0 + "," + source.x0 + ")";
			})
			.on("click", toggle);

		nodeEnter.append("circle")
			.attr("r", 1e-6)
			.style("fill", function (d) {
				return d._children ? "lightsteelblue" : "#fff";
			});

		nodeEnter.append("text")
			.attr("x", function (d) {
				return d.children || d._children ? -13 : 13;
			})
			.attr("dy", ".35em")
			.attr("text-anchor", function (d) {
				return d.children || d._children ? "end" : "start";
			})
			.text(function (d) {
				return d.name;
			})
			.style("fill-opacity", 1e-6);

		// Transition nodes to their new position.
		var nodeUpdate = node.transition()
			.duration(duration)
			.attr("transform", function (d) {
				return "translate(" + d.y + "," + d.x + ")";
			});

		nodeUpdate.select("circle")
			.attr("r", 10)
			.style("fill", function (d) {
				return d._children ? "lightsteelblue" : "#fff";
			});

		nodeUpdate.select("text")
			.style("fill-opacity", 1);

		// Transition exiting nodes to the parent's new position.
		var nodeExit = node.exit().transition()
			.duration(duration)
			.attr("transform", function (d) {
				return "translate(" + source.y + "," + source.x + ")";
			})
			.remove();

		nodeExit.select("circle")
			.attr("r", 1e-6);

		nodeExit.select("text")
			.style("fill-opacity", 1e-6);

		// Update the links…
		var link = svg.selectAll("path.link")
			.data(links, function (d) {
				return d.target.id;
			});

		// Enter any new links at the parent's previous position.
		link.enter().insert("path", "g")
			.attr("class", "link")
			.attr("d", function (d) {
				var o = {x: source.x0, y: source.y0};
				return diagonal({source: o, target: o});
			});

		// Transition links to their new position.
		link.transition()
			.duration(duration)
			.attr("d", diagonal);

		// Transition exiting nodes to the parent's new position.
		link.exit().transition()
			.duration(duration)
			.attr("d", function (d) {
				var o = {x: source.x, y: source.y};
				return diagonal({source: o, target: o});
			})
			.remove();

		// Stash the old positions for transition.
		nodes.forEach(function (d) {
			d.x0 = d.x;
			d.y0 = d.y;
		});
	}

// Toggle children on click.
	function toggle(d) {
		if (d.children) {
			d._children = d.children;
			d.children = null;
		} else {
			d.children = d._children;
			d._children = null;
		}
		update(d);
	}

	function toggleAll(d) {
		if (d.children) {
			d.children.forEach(toggleAll);
			toggle(d);
		}
	}

});

