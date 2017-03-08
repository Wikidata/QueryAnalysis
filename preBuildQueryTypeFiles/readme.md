- name.preBuildQueryType-files in this folder will be imported as queryTypes and name will be used as the query type
- queries in .preBuildQueryType-files do not need to be normalized, they will be normalized during loading
- the file toolMapping.tsv can be filled with mappings of the following form:
queryType   userAgent   toolName    toolVersion (comment) (using tab as a delimiter, ignoring the first row)

NOTE: Since the OpenRDF-Renderer misses out on a lot of features it is recommended to supply an example query for query type rather than using the data from the .queryType-files.

NOTES ON QUERY TYPES:
user agents that are probably too broad:
- Apache-Jena-ARQ
- Java
- [sparqlwrapper](http://rdflib.github.io/sparqlwrapper/)
- Ruby

NOTES ON POSSIBLY SEMI-AUTOMATED SOURCES:
- wikidataLastModified could be semi-automated (lots of different browser/os combinations, always the same query)
