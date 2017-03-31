import csv

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


def plotHist(X, Y, title):
    plt.figure(figsize=(10, 6))
    plt.subplot(111)
    plt.grid(True)

    plt.title(title)
    plt.xlabel("count of triples")
    plt.ylabel("count of queries")

    axes = plt.axes()
    axes.xaxis.set_major_locator(ticker.MultipleLocator(1))

    axes.set_yscale('log')

    plt.bar(X, Y, facecolor='#9999ff', edgecolor='white')

    plt.xlim(-1, 31)

    plt.savefig('tripleCount/figures/' + title + ".png", dpi=100)
    plt.close()


for j in xrange(1, 3):
    files = []
    for i in xrange(1, 31):
        if j == 1:
            files.append("tripleCount/QueryProcessedJena" + "%02d" % i + "TripleCountSpider.tsv")
            type = "Spider"
        elif j == 2:
            files.append("tripleCount/QueryProcessedJena" + "%02d" % i + "TripleCountUser.tsv")
            type = "User"

    daily_Xs = []
    daily_Ys = []

    for file in sorted(files):
        print "Working on: " + file
        with open(file) as tsvFile:
            tsvFile = csv.reader(tsvFile, delimiter='\t')

            # skip tsv header
            next(tsvFile, None)

            X = []
            Y = []
            for row in tsvFile:
                X.append(int(row[0]))
                Y.append(int(row[1]))

            daily_Xs.append(X)
            daily_Ys.append(Y)
            plotHist(X, Y, file[file.rfind("/"):])

    monthly_X = []
    monthly_Y = []

    for X, Y in zip(daily_Xs, daily_Ys):
        for x, y in zip(X, Y):
            if x not in monthly_X:
                monthly_X.append(x)
                monthly_Y.append(y)
            else:
                monthly_Y[monthly_X.index(x)] += y

    plotHist(monthly_X, monthly_Y, "QueryProcessedJenaMonthlyTripleCount" + type)
