import csv
from pprint import pprint
from collections import defaultdict

st = []
for i in xrange(1, 31):
    st.append("QueryProcessedOpenRDF" + "%02d" % i + ".tsv")

for fileName in sorted(st):

            print "Working on: " + fileName
            with open(fileName) as tsvFile:
                tsvFile = csv.reader(tsvFile, delimiter='\t')

                # skip tsv header
                next(tsvFile, None)

                data = defaultdict(int)
                for row in tsvFile:
                    if row[1] == "-1":
                        continue
                    data[row[7]] += 1

                pprint(sorted(data.iteritems(), key=lambda x:x[1], reverse=True))