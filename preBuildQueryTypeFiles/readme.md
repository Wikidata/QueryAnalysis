- name.preBuildQueryType-files in this folder will be imported as queryTypes and name will be used as the query type
- queries in .preBuildQueryType-files do not need to be normalized, they will be normalized during loading
- the file toolMapping.tsv can be filled with mappings of the following form:
queryType   userAgent   toolName    toolVersion (comment) (using tab as a delimiter, ignoring the first row)

NOTE: Since the OpenRDF-Renderer misses out on a lot of features it is recommended to supply an example query for query type rather than using the data from the .queryType-files.

NOTES ON QUERY TYPES:
user agents that are probably too broad:
- Apache-Jena-ARQ
- Java
- sparqlwrapper
- Ruby

NOTES ON POSSIBLY SEMI-AUTOMATED SOURCES:
- query type 12 comes from a lot of different browsers but is about when something was most recently modified, so it might be called each time someone opens e.g. a wikipedia page
- processed including 23324 in TotalQueryTypeCountUser.tsv
