import csv
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

matplotlib.use("Agg")

folder = "userAgent"
metric = "UserAgentCount"

def plotHist(X, Y, title):
    plt.figure(figsize=(10,6))
    plt.subplot(111)
    plt.grid(True)

    plt.title(title)
    plt.xlabel(folder)
    plt.ylabel("count of queries")

    axes = plt.axes()
    axes.set_yscale('log')

    plt.bar(X, Y, facecolor='#9999ff', edgecolor='#9999ff')

    plt.xlim(0)
    #plt.show()
    plt.savefig(folder + "/figures/" + title + ".png", dpi=100)
    plt.close()

for j in xrange(0,2):
	files = []
	for i in xrange(1, 31):
        	if j == 0:
            		files.append(folder + "/QueryProcessedJena" + "%02d" % i + metric + "Spider.tsv")
		elif j == 1:
			files.append(folder + "/QueryProcessedJena" + "%02d" % i + metric + "User.tsv")

	files.append(folder + "TotalUserAgentCountSpider.tsv")
	files.append(folder + "TotalUserAgentCountUser.tsv")

	for file in sorted(files):
		print "Working on: " + file
		with open(file) as tsvFile:
			tsvFile = csv.reader(tsvFile, delimiter='\t')

			# skip tsv header
			next(tsvFile, None)

			X = []
			Y = []
			for row in tsvFile:
				X.append(row[0])
				Y.append(int(row[1]))

			plotHist(X, Y, file[file.rfind("/"):])
