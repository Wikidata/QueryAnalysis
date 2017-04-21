# Usage of python tools
## [extractUserData.py](extractUserData.py)
Creates a folder named `userData` containing only `queryCnt01.tsv` and `QueryProcessedOpenRDF01.tsv` files, which have `USER` as their `ToolName`. 
Adds while doing so the original URL encoded query string to `QueryProcessedOpenRDF01.tsv`.

## [getClassifiedBotsData.py](getClassifiedBotsData.py)
You need to specify the directory which contains the `QueryProcessedOpenRDF01.tsv` as the input parameter, and in the code the metric you want to be extracted. 
The output will be, in the directory `classifiedBotsData` for each day a `01ClassifiedBotsData.tsv` and a `TotalClassifiedBotsData.tsv`. 
These files contain three columns, first the hour, then the specified metric and as the third the count.

## [plotClassifiedBotsData.py](plotClassifiedBotsData.py)
Takes the created files from `getClassifiedBotsData.py` as input and creates stacked bar charts out of them. Creates a log and a linear version.