#!/bin/bash
# must be executed from inside of the tools folder
WORKING_DIR=$1
MONTH=$2

# extract datasets
python2 extractUserData.py -m $1 $2
python2 extractUniqueDataset.py -m $1/$2 userData
python2 extractQueryTypeDataset.py -m $1/$2 userData

# calculate stats
mkdir -p logs/sp
mkdir -p logs/op

function analyzeMonth() {
    python2 getSparqlStatistic.py -m $1 $2 > logs/sp/sparqlStatistic$3$2.txt
    python2 operatorUsageStatistic.py -m $1 $2 > logs/op/operatorUsageStatistic$3$2.txt 
    python2 generalStat.py -m $1 $2 > logs/generalStat$3$2.txt 
}

analyzeMonth $1 $2 $2
analyzeMonth $1/$2 userData $2
analyzeMonth $1/$2/userData queryTypeDataset $2
analyzeMonth $1/$2/userData uniqueQueryDataset $2
