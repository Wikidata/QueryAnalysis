import csv
import os
import csv
from pprint import pprint

import matplotlib.pyplot as plt



def plotHist(X, Y, title):
    plt.figure(figsize=(10, 6))
    plt.subplot(111)
    plt.grid(True)

    plt.title(title)
    plt.xlabel("count of triples")
    plt.ylabel("count of queries")

    axes = plt.axes()
    #axes.set_yscale('log')

    plt.bar(X, Y, facecolor='#9999ff', edgecolor='#9999ff')

    plt.xlim(0)
    #plt.show()
    plt.savefig(title + ".png", dpi=100)
    plt.close()

metrics = ['StringLengthNoComments', 'VariableCountPattern']
for metric in metrics:

    files = []

    for i in xrange(1,2):
        files.append(metric +  '/Day' + "%02d"%i + "Hourly" + metric + ".tsv")
    files.append(metric +  '/TotalHourly' + metric + ".tsv")

    for file in files:
        print "Working on: " + file
        with open(file) as f:
            reader = csv.DictReader(f, delimiter="\t")
            for line in reader:
                X = list(line.keys())
                X.remove('hour')
                Y = []
                for x in X:
                    Y.append(int(line[x]))
                if line['hour'] == '10':
                    plotHist(X,Y, file +'Plot' + line['hour'])