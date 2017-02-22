import csv
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.pyplot import cm
import numpy as np
import matplotlib.colors as mcolors
from pprint import pprint


def plotHist(data, title, countTools):
    plt.figure()
    plt.subplot(111)
    plt.subplots_adjust(right=0.7)
    plt.grid(True)

    plt.title(title)
    plt.xlabel("hour")
    plt.ylabel("count")

    axes = plt.axes()
    axes.xaxis.set_major_locator(ticker.MultipleLocator(1))

    #axes.set_yscale('log')

    color = iter(cm.rainbow(np.linspace(0, 1, countTools)))

    for toolName, XY in data.iteritems():
        c = next((color))
        try:
            plt.bar(XY["X"], XY["Y"], align='center', color=c, edgecolor=c, label=toolName + str(XY["Y"]))
        except ValueError:
            pass

    plt.legend(bbox_to_anchor=(1, 1), loc='upper left', ncol=1, prop={'size':6})


    plt.xlim(0, 25)
    plt.xticks(fontsize=9)
    plt.savefig("classifiedBotsData/" + title + ".png", dpi=120)
    plt.close()


inputDirectory = "dayTriple/"

files = []
for i in xrange(1,2):
    files.append("classifiedBotsData/" + "%02d"%i + "ClassifiedBotsData.tsv")
files.append("classifiedBotsData/TotalClassifiedBotsData.tsv")

for file in files:
    print "Working on: " + file
    with open(file) as f:
        reader = csv.DictReader(f, delimiter="\t")

        hours = []
        toolNames = []
        counts = []

        for line in reader:
            hours.append(int(line['hour']))
            toolNames.append(line['ToolName'])
            counts.append(int(line['count']))

        #divide data into "stacks"

        data = {}
        for toolName in toolNames:
            data[toolName] = {}
            data[toolName]["X"] = list()
            data[toolName]["Y"] = list()

        for hour, toolName, count in zip(hours, toolNames, counts):
            data[toolName]["X"].append(hour)
            data[toolName]["Y"].append(count)

        plotHist(data, file[file.rfind("/"):], len(toolNames))