# Usage of python tools
## [extractUserData.py](extractUserData.py)
Creates a folder named `userData` containing only `queryCnt01.tsv` and `QueryProcessedOpenRDF01.tsv` files, which have `USER` as their `ToolName`. 
Adds while doing so the original URL encoded query string to `QueryProcessedOpenRDF01.tsv`.

## [getClassifiedBotsData.py](getClassifiedBotsData.py)
You need to specify the directory which contains the `QueryProcessedOpenRDF01.tsv` as the first input parameter and the metric you want to be extracted as the second. 
The output will be, in the directory `classifiedBotsData` for each day a `01ClassifiedBotsData.tsv` and a `TotalClassifiedBotsData.tsv`. 
These files contain three columns, first the hour, then the specified metric and as the third the count.

## [plotClassifiedBotsData.py](plotClassifiedBotsData.py)
You need to specify the directory which contains the result from `getClassifiedBotsData.py` as the first input parameter, the metric you want to be extracted as the second. 
Takes the created files from `getClassifiedBotsData.py` as input and creates stacked bar charts out of them. Creates a log and a linear version, daily and hourly versions.

## [visualizeSparqlStatistic.py](visualizeSparqlStatistic.py)
Takes the `QueryProcessedOpenRDF01.tsv` directory as the first input parameter and prints out the SPARQL feature statistic in the following way:
```
#And                              973/122243     0.8%
#ArbitraryLengthPath            46945/122243    38.4%
#Avg                                4/122243     0.0%
#BindingSetAssignment            1309/122243    1.07%
[…]
```