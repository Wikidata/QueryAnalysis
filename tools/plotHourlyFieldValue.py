import csv
import os
from collections import defaultdict
from pprint import pprint

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


def plotHist(X, Y, title, xlabel="x", ylabel="count of queries", log=False):
    if not os.path.exists(title[:title.rfind("/")]):
        os.makedirs(title[:title.rfind("/")])

    plt.figure(figsize=(10, 6))
    plt.subplot(111)
    plt.grid(True)

    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    axes = plt.axes()

    if log:
        axes.set_yscale('log')

    if X is None:
        X = range(len(Y))
        Y = Y.values()

    minX = min(X)
    maxX = max(X)
    plt.bar(X, Y, align='center', facecolor='#9999ff', edgecolor='#9999ff')

    plt.xlim(minX - 1, maxX + 1)

    if xlabel is "Hour" or xlabel is "Day":
        axes.xaxis.set_major_locator(ticker.MultipleLocator(1))

    # plt.xlim(0)
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

    totalMetrics = []
    totalQueryCountPerDay = defaultdict(int)
    totalTotalQueryCountPerMetric = defaultdict(int)

    for file in files:
        totalQueryCountPerHour = defaultdict(int)
        totalQueryCountPerMetric = defaultdict(int)

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

                X = [int(x) for x in X]

                plotHist(X, Y, metric + '/plots/day' + day + '/hour' + line['hour'], xlabel=metric)
                plotHist(X, Y, metric + '/plots/day' + day + '/log/hour' + line['hour'], xlabel=metric, log=True)

        sum = 0
        for hour in totalQueryCountPerHour:
            sum += totalQueryCountPerHour[hour]
        totalQueryCountPerDay[int(day)] += sum

        for m in totalQueryCountPerMetric:
            totalTotalQueryCountPerMetric[m] += totalQueryCountPerMetric[m]

        plotHist(None, totalQueryCountPerHour, metric + '/plots/day' + day + '/totalQueryCountPerHour', xlabel="Hour")
        plotHist(None, totalQueryCountPerMetric, metric + '/plots/day' + day + '/totalQueryCountPer' + metric,
                 xlabel=metric)

    pprint(totalQueryCountPerMetric)
    pprint(totalTotalQueryCountPerMetric)

    plotHist(None, totalTotalQueryCountPerMetric, metric + '/plots/totalQueryCountPer' + metric, xlabel=metric)
    plotHist(None, totalQueryCountPerDay, metric + '/plots/totalQueryCountPerDay', xlabel="Day")
