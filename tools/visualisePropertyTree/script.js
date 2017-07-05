"use strict";
$('#propertyTreeTable').treetable({
	expandable: true,
	clickableNodeNames: true
});

$('#propertyTreeTable').treetable("expandNode", "/http://www.wikidata.org/entity/Q18616576");