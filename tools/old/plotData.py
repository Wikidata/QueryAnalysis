import csv
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.pyplot import cm
import numpy as np
import matplotlib.colors as mcolors


def plotHist(data, title, maxMetric):
    plt.figure()
    plt.subplot(111)
    plt.grid(True)

    plt.title(title)
    plt.xlabel(timeName)
    plt.ylabel("count")

    axes = plt.axes()
    axes.xaxis.set_major_locator(ticker.MultipleLocator(1))

    axes.set_yscale('log')

    color = iter(cm.rainbow(np.linspace(0, 1, maxMetric)))

    for metric, XY in data.iteritems():
        c = next((color))
        try:
            plt.bar(XY["X"], XY["Y"], align='center', color=c, edgecolor=c)
        except ValueError:
            pass


    colormap = cm.rainbow
    normalize = mcolors.Normalize(vmin=0, vmax=maxMetric)

    scalarmappable = cm.ScalarMappable(norm=normalize, cmap=colormap)
    scalarmappable.set_array(np.arange(0,maxMetric))
    plt.colorbar(scalarmappable)


    plt.xlim(-1, xEnd)
    plt.xticks(fontsize=9)

    plt.savefig(inputDirectory + title + ".png", dpi=120)
    plt.close()


for a in range(0,2):
    if a == 0:
        metricName = "Stringlengthnocomments"
    else:
        metricName = "TripleCount"
    for b in range(0,2):
        if b == 0:
            agent = "spider"
        else:
            agent = "user"

        for c in range(0,2):
            if c == 0:
                day = True
            else:
                day = False

            inputDirectory = "dayTriple/"
            #metricName = "Stringlengthnocomments"

            #agent = "spider"
            #agent = "user"

            #day = False

            if day:
                timeName = "Day"
                start = 1
                end = 31
                xEnd = 32
            else:
                timeName = "Hour"
                start = 0
                end = 24
                xEnd = 24



            fileName = inputDirectory + agent + timeName + metricName + ".tsv"

            print "Working on: " + fileName
            with open(fileName) as tsvFile:
                tsvFile = csv.reader(tsvFile, delimiter='\t')

                # skip tsv header
                next(tsvFile, None)

                allTimes = []
                allMetrics = []
                allCounts = []

                maxMetric = 0

                for row in tsvFile:
                    if row[1] == "-1":
                        continue
                    metric = int(row[1])
                    if metric > maxMetric:
                        maxMetric = metric
                    allTimes.append(int(row[0]))
                    allMetrics.append(metric)
                    allCounts.append(int(row[2]))

                #divide data into "stacks"

                data = {}
                for i in range(0,maxMetric + 1):
                    data[i] = {}
                    data[i]["X"] = list()
                    data[i]["Y"] = list()

                for time, metric, count in zip(allTimes, allMetrics, allCounts):
                    i = metric
                    data[i]["X"].append(time)
                    data[i]["Y"].append(count)

                #pprint(data)
                plotHist(data, fileName[fileName.rfind("/"):], maxMetric+1)