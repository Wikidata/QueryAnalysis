#!/usr/bin/env bash
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

python2 getSparqlStatistic.py -m $1/$2 userData > logs/sp/userDataSparqlStatistic.txt
python2 operatorUsageStatistic.py -m $1/$2 userData > logs/op/userDataOperatorUsageStatistic.txt 
python2 generalStat.py -m $1/$2 userData > logs/userDataGeneralStat.txt 

python2 getSparqlStatistic.py -m $1/$2/userData queryTypeDataset > logs/sp/queryTypeDatasetSparqlStatistic.txt
python2 operatorUsageStatistic.py -m $1/$2/userData queryTypeDataset > logs/op/queryTypeDatasetOperatorUsageStatistic.txt 
python2 generalStat.py -m $1/$2/userData queryTypeDataset > logs/queryTypeDatasetGeneralStat.txt 

python2 getSparqlStatistic.py -m $1/$2/userData uniqueDataset > logs/sp/uniqueDatasetSparqlStatistic.txt
python2 operatorUsageStatistic.py -m $1/$2/userData uniqueDataset > logs/op/uniqueDatasetOperatorUsageStatistic.txt 
python2 generalStat.py -m $1/$2/userData uniqueDataset > logs/uniqueDatasetGeneralStat.txt 
