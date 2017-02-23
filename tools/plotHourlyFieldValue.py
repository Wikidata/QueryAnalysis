import csv
import os
from pprint import pprint

import matplotlib.pyplot as plt


def plotHist(X, Y, title, metric, log=False):
    if not os.path.exists(title[:title.rfind("/")] ):
        os.makedirs(title[:title.rfind("/")] )

    plt.figure(figsize=(10, 6))
    plt.subplot(111)
    plt.grid(True)

    plt.title(title)
    plt.xlabel(metric)
    plt.ylabel("count of queries")

    axes = plt.axes()
    if log:
        axes.set_yscale('log')

    axes.set_ylim([0,50000])

    plt.bar(X, Y, facecolor='#9999ff', edgecolor='#9999ff')

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
    # files.append(metric +  '/TotalHourly' + metric + ".tsv")

    for file in files:
        print "Working on: " + file

        day = file.replace(metric + '/Day', '').replace("Hourly" + metric + ".tsv", '')

        with open(file) as f:
            reader = csv.DictReader(f, delimiter="\t")
            for line in reader:

                X = list(line.keys())
                X.remove('hour')
                Y = []
                for x in X:
                    Y.append(int(line[x]))

                plotHist(X,Y, metric + '/plots/day' + day + '/hour' + line['hour'], metric)
                plotHist(X,Y, metric + '/plots/day'+ day +  '/log/hour' + line['hour'], metric, True)
