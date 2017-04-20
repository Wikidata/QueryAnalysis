import csv
from shutil import copyfile

import os

subfolder = "userData/"

processedPrefix = "QueryProcessedOpenRDF"
sourcePrefix = "queryCnt"

if not os.path.exists(subfolder):
    os.makedirs(subfolder)

queryTypes = set()

for i in xrange(1, 2):
    with open(processedPrefix + "%02d" % i + ".tsv") as p, open(sourcePrefix + "%02d" % i + ".tsv") as s, open(
                                    subfolder + processedPrefix + "%02d" % i + ".tsv", "w") as user_p, open(
                                subfolder + sourcePrefix + "%02d" % i + ".tsv", "w") as user_s:
        pReader = csv.DictReader(p, delimiter="\t")
        sReader = csv.DictReader(s, delimiter="\t")

        pWriter = csv.DictWriter(user_p, None, delimiter="\t")
        sWriter = csv.DictWriter(user_s, None, delimiter="\t")

        for processed, source in zip(pReader, sReader):
            if pWriter.fieldnames is None:
                ph = dict((h, h) for h in pReader.fieldnames)
                ph['#uri_query'] = '#uri_query'
                ph['#hour'] = '#hour'
                ph['#agent_type'] = '#agent_type'
                ph['#ts'] = '#ts'
                pWriter.fieldnames = pReader.fieldnames
                pWriter.fieldnames.append('#uri_query')
                pWriter.fieldnames.append('#hour')
                pWriter.fieldnames.append('#agent_type')
                pWriter.fieldnames.append('#ts')
                pWriter.writerow(ph)

            if sWriter.fieldnames is None:
                sh = dict((h, h) for h in sReader.fieldnames)
                sWriter.fieldnames = sReader.fieldnames
                sWriter.writerow(sh)

            if (processed["#ToolName"] == "USER"):
                processed['#uri_query'] = source['uri_query']
                processed['#hour'] = source['hour']
                processed['#ts'] = source['ts']
                pWriter.writerow(processed)
                sWriter.writerow(source)
                queryTypes.add(processed["#QueryType"])

queryTypeFolder = "queryType/queryTypeFiles/"

if not os.path.exists(subfolder + queryTypeFolder):
    os.makedirs(subfolder + queryTypeFolder)

for queryType in queryTypes:
    original = queryTypeFolder + queryType + ".queryType"
    try:
        copyfile(original, "userData/" + original)
    except:
        print original + " does not exist."
