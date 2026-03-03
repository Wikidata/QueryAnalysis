from __future__ import print_function

import argparse
import glob
import gzip
import random
import sys
import urllib

from utility import utility

parser = argparse.ArgumentParser(description = "This script generates an example raw log file based on a folder with example queries")
parser.add_argument("--exampleQueryFolder", "-e", default="",
                        type=str, help="The folder in which the example queries are residing.")
parser.add_argument("--outputDirectory", "-o", default="",
                    type=str, help="The directory in which the QueryCnt01.tsv.gz-file should be created.")

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

uri_path = {"/sparql", "/bigdata/namespace/wdq/sparql"}
user_agent = {"Mozilla/5.0 (Android 4.4; Mobile; rv:41.0) Gecko/41.0 Firefox/41.0", "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:53.0) Gecko/20100101 Firefox/53.0"}
timestamp = {"2017-07-01 01:31:14", "2017-07-01 01:32:54", "2017-07-01 01:34:10"}
agent_type = {"spider", "user"}
http_status = "200"

with gzip.open(utility.addMissingSlash(args.outputDirectory) + "QueryCnt01.tsv.gz", "w") as target:
    print("uri_query\turi_path\tuser_agent\tts\tagent_type\thour\thttp_status", file = target)
    
    exampleQueryFolder = utility.addMissingSlash(args.exampleQueryFolder)
    
    for filename in glob.glob(exampleQueryFolder + "*.exampleQuery"):
        with open(filename) as exampleFile:
            line = "?query=" + urllib.quote_plus(exampleFile.read()) + "\t"
            line += random.sample(uri_path, 1)[0] + "\t"
            line += random.sample(user_agent, 1)[0] + "\t"
            line += random.sample(timestamp, 1)[0] + "\t"
            line += random.sample(agent_type, 1)[0] + "\t"
            line += str(random.randint(0,23)) + "\t"
            line += http_status
            print(line, file = target)
