import csv
from collections import defaultdict
from pprint import pprint

st = []
for i in xrange(1, 32):
    st.append("QueryProcessedOpenRDF" + "%02d" % i + ".tsv")

data = defaultdict(int)

for fileName in sorted(st):

    print "Working on: " + fileName
    with open(fileName) as tsvFile:
        tsvFile = csv.reader(tsvFile, delimiter='\t')

        # skip tsv header
        next(tsvFile, None)

        for row in tsvFile:
            if row[1] == "-1":
                continue
            data[row[7]] += 1

pprint(sorted(data.iteritems(), key=lambda x: x[1], reverse=True))
