import csv

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import os
from matplotlib.pyplot import cm


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

    for tool in data:
        c = next((color))

        toolName = tool[0]
        XY = tool[1]

        try:
            ax.bar(XY["X"], XY["Y"], align='center', color=c, edgecolor=c, label=toolName + str(XY["Y"]))
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
for i in xrange(1, 32):
    files.append("classifiedBotsData/" + "%02d" % i + "ClassifiedBotsData.tsv")


# files.append("classifiedBotsData/TotalClassifiedBotsData.tsv")

def compare(item1, item2):
    if max(item1[1]["Y"]) < max(item2[1]["Y"]):
        return 1
    elif max(item1[1]["Y"]) == max(item2[1]["Y"]):
        return 0
    else:
        return -1


totalDataPerDay = {}
totalToolNames = set()

for file in files:
    print "Working on: " + file

    day = file.replace('classifiedBotsData/', '').replace("ClassifiedBotsData.tsv", '')

    with open(file) as f:
        reader = csv.DictReader(f, delimiter="\t")

        hours = []
        toolNames = []
        counts = []

        for line in reader:
            if line['ToolName'] == '0':
                continue
            hours.append(int(line['hour']))
            toolNames.append(line['ToolName'])
            totalToolNames.add(line['ToolName'])
            counts.append(int(line['count']))

        # divide data into "stacks"

        data = {}
        for toolName in toolNames:
            if toolName not in totalDataPerDay:
                totalDataPerDay[toolName] = {}
                totalDataPerDay[toolName]["X"] = list()
                totalDataPerDay[toolName]["Y"] = list()

            data[toolName] = {}
            data[toolName]["X"] = list()
            data[toolName]["Y"] = list()

        for hour, toolName, count in zip(hours, toolNames, counts):
            data[toolName]["X"].append(hour)
            data[toolName]["Y"].append(count)

        # sort data so that the log graph is kind of useful
        sorted_data = sorted(data.items(), cmp=compare)

        plotHist(sorted_data, 'classifiedBotsData/plots/day' + day, len(set(toolNames)))
        plotHist(sorted_data, 'classifiedBotsData/plots/log/day' + day, len(set(toolNames)), log=True)

    for toolName in totalToolNames:
        if toolName in data:
            totalDataPerDay[toolName]["X"].append(int(day))

            sum = 0
            for count in data[toolName]["Y"]:
                sum += count
            totalDataPerDay[toolName]["Y"].append(sum)

sorted_data = sorted(totalDataPerDay.items(), cmp=compare)

plotHist(sorted_data, "classifiedBotsData/plots/total", len(totalToolNames), xlabel='day')
plotHist(sorted_data, "classifiedBotsData/plots/total_log", len(totalToolNames), xlabel='day', log=True)
