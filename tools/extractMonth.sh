#!/bin/bash

if [ ! $# -eq 3 ]
then
	echo "Arguments: <year> <month> <days>"
	exit
fi

found=false
month=0
declare -A months=( ["Jan"]="1" ["Feb"]="2" ["Mar"]="3" ["Apr"]="4" ["May"]="5" ["Jun"]="6" ["Jul"]="7" ["Aug"]="8" ["Sep"]="9" ["Oct"]="10" ["Nov"]="11" ["Dec"]="12" )
for m in "${!months[@]}";
do 
	if [ $2 = $m ]
	then
		month=${months[$m]}; 
		found=true
	fi
done

if [ $found = false ]
then
	echo "Month not in list. Use one of the following:"
	echo "Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec"
	exit
fi

filePrefix="queryCnt"
ext=".tsv"

for i in $(seq -f "%02g" 1 $3)
do
	 mkdir temp
	 hive -e "insert overwrite local directory 'temp' row format delimited fields terminated by '\t' select uri_query, uri_path, user_agent, ts, agent_type, hour, http_status from wmf.wdqs_extract where uri_query<>\"\" and year='$1' and month='$month' and day='$i'"
	 echo -e "uri_query\turi_path\tuser_agent\tts\tagent_type\thour\thttp_status" > $filePrefix$i$ext
	 cat temp/* >> $filePrefix$i$ext 	
	 rm -r temp
done
