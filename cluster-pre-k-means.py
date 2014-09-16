#!/usr/bin/env python
# coding: utf-8

import csv
import sys
import operator
import itertools as it
import numpy as np
from collections import defaultdict
from sklearn.cluster import KMeans
from time import strptime as time
from datetime import datetime as date

csv.field_size_limit(1000000000)

def parseZipCSV(fileName):
    csvfile = open(fileName, 'rb')
    reader = csv.reader(csvfile, delimiter = ',')

    return dict(map(lambda row: (row[0], row[2]), reader))

def parseCSV(fileName):
    delimiter = u'\u001D'.encode('utf-8')

    csvfile = open(fileName, 'rb')
    reader = csv.reader(csvfile, delimiter = delimiter)
    rows1, rows2 = it.tee(reader)
    # events = it.imap(lambda row: (row[9], row[2], row[17] + ':' + row[27] + ':' + row[28]), rows1)
    events = it.imap(lambda row: (row[9], row[2], row[17], row[12]), rows1)
    # templates = it.imap(lambda row: row[17] + ':' + row[27] + ':' + row[28], rows2)
    templates = it.imap(lambda row: row[17], rows2)
    return (set(templates), it.groupby(events, lambda t: t[0]))

def timeSpent(events):
    def parseTime(t):
        return date(*(time(t, '%H:%M:%S')[0:6]))

    def diffTime(before, after):
        return (parseTime(after) - parseTime(before)).seconds

    prev = None
    for event in events:
        if prev is not None:
            yield (prev[2], diffTime(prev[1], event[1]), prev[3])
        prev = event
    if prev is not None:
        yield (prev[2], 0, prev[3])

def sumTime(events):
    for event, group in it.groupby(events, lambda e: e[0]):
        yield (event, reduce(lambda result, e: (result[0] + e[1], e[2]), group, (0, 0)))

def construnctFeatures(stream):
    for user, events in stream:
        yield (user, sumTime(timeSpent(events)))

def buildVectors(eventList, visitStream):
    for user, events in construnctFeatures(visitStream):
        vector = [0] * (len(eventList) + 1)
        for event, (time, zCode) in events:
            vector[eventList.index(event)] = time
            vector[-1] = zCode
        yield vector

def clusterize(features):
    features = np.array(features)

    k_means = KMeans(init='k-means++', n_clusters=103, n_jobs=6)
    k_means.fit(features)
    k_means_labels = k_means.labels_
    for k in range(103):
        yield features[k_means_labels == k]

def main(args):
    eventTypes, visitStream = parseCSV(args[1])
    zipToSalary = parseZipCSV(args[2])

    eventList = list(eventTypes)
    featureVectors = list(buildVectors(eventList, visitStream))

    #add salary info
    for feature in featureVectors:
        if feature[-1] in zipToSalary:
            feature[-1] = int(zipToSalary[feature[-1]])
        else:
            feature[-1] = 0

    for cluster in clusterize(featureVectors):
        behaviour = defaultdict(lambda: (0, 0.0))
        meanSalary = 0
        for sample in cluster:
            for i, feature in zip(range(len(sample) - 1), sample):
                if feature > 0:
                    time, count = behaviour[eventList[i]]
                    behaviour[eventList[i]] = (feature + time, count + 1)
            meanSalary += sample[-1]
        meanSalary /= len(cluster)
        percent = len(cluster) * 100.0 / len(featureVectors)
        if len(cluster) >= 100:
            if percent >= 1:
                print percent, len(cluster), meanSalary
                for page, time in reversed(sorted(behaviour.iteritems(),
                                                key=operator.itemgetter(1))):
                    print '\t', page, time[0] / time[1]
                print

if __name__ == "__main__":
    main(sys.argv)
