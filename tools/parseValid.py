from collections import defaultdict

for xkcd in range(2):
	st = []
	if xkcd == 0:
		for i in xrange(1, 31):
			st.append("QueryProcessedJena" + "%02d" % i + ".tsv")

	if xkcd == 1:
		for i in xrange(1, 31):
			st.append("QueryProcessedOpenRDF" + "%02d" % i + ".tsv")

	totalValidities = defaultdict(int)
	totalTotalCount = 0

	for file in sorted(st):
		print "Working on: " + file
		with open(file) as f:
			validityCodeCounts = defaultdict(int)
			totalCount = 0
			for line_no, line in enumerate(f):
				if line_no == 0:
					continue
				totalCount += 1
				validityCode = int(line.split()[0])
				validityCodeCounts[validityCode] += 1
			for k, v in validityCodeCounts.iteritems():
				totalValidities[k] += v
				percentage = float(v) / totalCount * 100
				print k, "\t", v, "\t", percentage
			totalTotalCount += totalCount

	print "Total: "
	for k, v in totalValidities.iteritems():
		percentage = float(v) / totalTotalCount * 100
		print k, "\t", v, "\t", percentage
