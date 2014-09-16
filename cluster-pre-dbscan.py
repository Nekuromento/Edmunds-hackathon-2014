#!/usr/bin/env python
# coding: utf-8

import csv
import sys
import operator
import itertools as it
import numpy as np
from collections import defaultdict
from sklearn import mixture
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
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

def buildVectors(eventList, visitStream):
    for user, events in construnctFeatures(visitStream):
        vector = [0] * len(eventList)
        for event, time in events:
            vector[eventList.index(event)] = time
        yield vector

def clusterize(features):
    features = np.array(list(features))
    #features = StandardScaler().fit_transform(features)

    db = mixture.GMM(n_components=10, covariance_type='full')
    db.fit(features)
    c = db.predict(features)
    for k in range(10):
        yield features[c == k]

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
        percent = len(cluster) * 100.0 / 243587
        if len(cluster) >= 100:
            print percent, len(cluster)
            for page, time in reversed(sorted(behaviour.iteritems(),
                                              key=operator.itemgetter(1))):
                print '\t', page, time[0] / time[1]
            print

if __name__ == "__main__":
    main(sys.argv)
