import csv
import os
from collections import defaultdict
from pprint import pprint
import matplotlib.ticker as ticker

import matplotlib.pyplot as plt


def plotHist(X, Y, title, xlabel="x", ylabel= "count of queries", log=False):
    if not os.path.exists(title[:title.rfind("/")] ):
        os.makedirs(title[:title.rfind("/")] )

    plt.figure(figsize=(10, 6))
    plt.subplot(111)
    plt.grid(True)

    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    axes = plt.axes()

    if xlabel is "Hour":
        axes.xaxis.set_major_locator(ticker.MultipleLocator(1))

    if log:
        axes.set_yscale('log')


    if X is not None:
        plt.bar(X, Y, align='center', facecolor='#9999ff', edgecolor='#9999ff')
        #axes.set_ylim([0,50000])
    else:
        plt.bar(range(len(Y)), Y.values(), facecolor='#9999ff', edgecolor='#9999ff')

    plt.xlim(0)
    # plt.show()
    plt.savefig(title + ".png")
    plt.close()


metrics = ['VariableCountPattern', 'StringLengthNoComments']
for metric in metrics:

    if not os.path.exists(metric + '/plots/log'):
        os.makedirs(metric + '/plots/log')

    files = []

    for i in xrange(1, 32):
        files.append(metric + '/Day' + "%02d" % i + "Hourly" + metric + ".tsv")

    totalHours = []
    totalQueryCountPerHour = defaultdict(int)

    totalMetrics =[]
    totalQueryCountPerMetric = defaultdict(int)

    for file in files:
        print "Working on: " + file

        day = file.replace(metric + '/Day', '').replace("Hourly" + metric + ".tsv", '')

        with open(file) as f:
            reader = csv.DictReader(f, delimiter="\t")
            for line in reader:
                totalHours.append(line['hour'])

                X = list(line.keys())
                X.remove('hour')

                totalMetrics = X

                Y = []
                for x in X:
                    Y.append(int(line[x]))
                    totalQueryCountPerHour[int(line['hour'])] += int(line[x])
                    totalQueryCountPerMetric[int(x)] += int(line[x])

                #plotHist(X,Y, metric + '/plots/day' + day + '/hour' + line['hour'], xlabel=metric)
                #plotHist(X,Y, metric + '/plots/day'+ day +  '/log/hour' + line['hour'], xlabel=metric, log=True)
    plotHist(None, totalQueryCountPerHour, metric + '/plots/totalQueryCountPerHour', xlabel="Hour")
    plotHist(None, totalQueryCountPerMetric, metric + '/plots/totalQueryCountPer' + metric, xlabel=metric)