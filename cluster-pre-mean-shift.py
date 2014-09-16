#!/usr/bin/env python
# coding: utf-8

import csv
import sys
import itertools as it
import numpy as np
from sklearn import preprocessing
from sklearn.cluster import MeanShift, estimate_bandwidth
from time import strptime as time
from datetime import datetime as date

csv.field_size_limit(1000000000)

def parseCSV(fileName):
    delimiter = u'\u001D'.encode('utf-8')

    csvfile = open(fileName, 'rb')
    reader = csv.reader(csvfile, delimiter = delimiter)
    rows1, rows2 = it.tee(reader)
    # events = it.imap(lambda row: (row[9], row[2], row[17] + ':' + row[27] + ':' + row[28]), rows1)
    events = it.imap(lambda row: (row[9], row[2], row[17]), rows1)
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
            yield (prev[2], diffTime(prev[1], event[1]))
        prev = event
    if prev is not None:
        yield (prev[2], 0)

def sumTime(events):
    for event, group in it.groupby(events, lambda e: e[0]):
        yield (event, reduce(lambda result, e: result + e[1], group, 0))

def construnctFeatures(stream):
    for user, events in stream:
        yield (user, sumTime(timeSpent(events)))

def buildVectors(eventTypes, visitStream):
    eventList = list(eventTypes)
    for user, events in construnctFeatures(visitStream):
        vector = [0] * len(eventTypes)
        for event, time in events:
            vector[eventList.index(event)] = time
        yield vector

def clusterize(features):
    features = np.array(list(features))
    bandwidth = estimate_bandwidth(features, quantile=0.2)

    ms = MeanShift(bandwidth=bandwidth, bin_seeding=True)
    ms.fit(features)
    labels = ms.labels_
    cluster_centers = ms.cluster_centers_

    labels_unique = np.unique(labels)
    n_clusters_ = len(labels_unique)
    for k in range(n_clusters):
        yield features[k_means_labels == k]

def main(args):
    eventTypes, visitStream = parseCSV(args[1])
    eventList = list(eventTypes)
    featureVectors = buildVectors(eventList, visitStream)
    for cluster in clusterize(featureVectors):
        behaviour = defaultdict(lambda: (0, 0.0))
        for sample in cluster:
            for i, feature in zip(range(len(sample)), sample):
                if feature > 0:
                    time, count = behaviour[eventList[i]]
                    behaviour[eventList[i]] = (feature + time, count + 1)
        percent = len(cluster) * 100.0 / 30644
        if percent >= 1:
            print percent
            for page, time in reversed(sorted(behaviour.iteritems(),
                                              key=operator.itemgetter(1))):
                print '\t', page, time[0] / time[1]
            print

if __name__ == "__main__":
    main(sys.argv)
