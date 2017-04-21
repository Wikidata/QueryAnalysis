import csv

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import os
import sys
from matplotlib.pyplot import cm

workingDir = sys.argv[1]
os.chdir(workingDir)

metricName = sys.argv[2]

def plotHist(data, title, countTools, xlabel="hour", ylabel="count of queries", log=False):
    if not os.path.exists(title[:title.rfind("/")]):
        os.makedirs(title[:title.rfind("/")])
    fig = plt.figure(1)
    ax = fig.add_subplot(111)
    plt.grid(True)

    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    axes = plt.axes()
    axes.xaxis.set_major_locator(ticker.MultipleLocator(1))

    if log:
        axes.set_yscale('log')

    color = iter(cm.nipy_spectral(np.linspace(0, 1, countTools)))

    for metric in data:
        c = next((color))

        metricName = metric[0]
        XY = metric[1]

        try:
            ax.bar(XY["X"], XY["Y"], align='center', color=c, edgecolor=c, label=metricName + str(XY["Y"]))
        except ValueError:
            pass


    handles, labels = ax.get_legend_handles_labels()
    lgd = ax.legend(handles, labels, bbox_to_anchor=(1, 1), loc='upper left', ncol=1, prop={'size': 6})


    if xlabel is 'hour':
        plt.xlim(-1, 24)

    if xlabel is 'day':
        plt.xlim(0, 32)

    plt.xticks(fontsize=9)

    plt.savefig(title + ".png", bbox_extra_artists=(lgd,), bbox_inches='tight')

    plt.close()


inputDirectory = "dayTriple/"

files = []
for i in xrange(1, 2):
    files.append("classifiedBotsData/" + metricName + "/" + "%02d" % i + "ClassifiedBotsData.tsv")


# files.append("classifiedBotsData/TotalClassifiedBotsData.tsv")

def compare(item1, item2):
    if max(item1[1]["Y"]) < max(item2[1]["Y"]):
        return 1
    elif max(item1[1]["Y"]) == max(item2[1]["Y"]):
        return 0
    else:
        return -1


totalDataPerDay = {}
totalMetricNames = set()

for file in files:
    print "Working on: " + file

    day = file.replace('classifiedBotsData/' + metricName + '/', '').replace("ClassifiedBotsData.tsv", '')

    with open(file) as f:
        reader = csv.DictReader(f, delimiter="\t")

        hours = []
        metrics = []
        counts = []

        for line in reader:
            if line[metricName] == '0':
                continue
            hours.append(int(line['hour']))
            metrics.append(line[metricName])
            totalMetricNames.add(line[metricName])
            counts.append(int(line['count']))

        # divide data into "stacks"

        data = {}
        for metric in metrics:
            if metric not in totalDataPerDay:
                totalDataPerDay[metric] = {}
                totalDataPerDay[metric]["X"] = list()
                totalDataPerDay[metric]["Y"] = list()

            data[metric] = {}
            data[metric]["X"] = list()
            data[metric]["Y"] = list()

        for hour, metric, count in zip(hours, metrics, counts):
            data[metric]["X"].append(hour)
            data[metric]["Y"].append(count)

        # sort data so that the log graph is kind of useful
        sorted_data = sorted(data.items(), cmp=compare)

        plotHist(sorted_data, 'classifiedBotsData/' + metricName + '/plots/day' + day, len(set(metrics)))
        plotHist(sorted_data, 'classifiedBotsData/' + metricName + '/plots/log/day' + day, len(set(metrics)), log=True)

    for metric in totalMetricNames:
        if metric in data:
            totalDataPerDay[metric]["X"].append(int(day))

            sum = 0
            for count in data[metric]["Y"]:
                sum += count
            totalDataPerDay[metric]["Y"].append(sum)

sorted_data = sorted(totalDataPerDay.items(), cmp=compare)

plotHist(sorted_data, "classifiedBotsData/" + metricName + "/plots/total", len(totalMetricNames), xlabel='day')
plotHist(sorted_data, "classifiedBotsData/" + metricName + "/plots/total_log", len(totalMetricNames), xlabel='day',
         log=True)
