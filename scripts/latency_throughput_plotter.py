#!/usr/bin/env python

import argparse
import re
from datetime import datetime
from pathlib import Path

import matplotlib.cbook as cbook
import matplotlib.collections as collections
import matplotlib.dates as matdates
import matplotlib.pyplot as plt
import matplotlib.transforms as mtransforms
import numpy as np
import pandas as pd
from matplotlib.ticker import AutoMinorLocator, MultipleLocator

parser = argparse.ArgumentParser()
parser.add_argument('--dir', type=str, required=True)

args = parser.parse_args()

parallel = None
read = None
conflict = None
run = None
nclients = []
for path in Path(args.dir).rglob('*.log'):
    c = re.findall('clients=([0-9]+)', str(path))[0]
    if int(c) not in nclients:
        nclients.append(int(c))
    parallel = re.findall('partitioned=(true|false)', str(path))[0]
    read = re.findall('read=([0-9]+)', str(path))[0]
    conflict = re.findall('conflict=([0-9]+)', str(path))[0]
    run = re.findall('run=([0-9]+)', str(path))[0]

nclients.sort()

PERCENTILE=95

latency = []
for client in nclients:
    print(client)
    for path in Path(args.dir).rglob('clients=' + str(client) + '/**/client.log'):
        with open(path) as file:
            lats = []
            for line in file:
                rs = re.findall('95th percentile for [0-9]+ executions = ([0-9]+) us', line)
                if rs:
                    lats.append(int(rs[0]))
        latency.append(np.percentile(lats,PERCENTILE))

throughput_reqsec = []
for client in nclients:
    throughput_by_client = []
    for path in Path(args.dir).rglob('clients=' + str(client) + '/**/throughput_*.log'):
        node = re.findall('throughput_([0-9]{3}).log', str(path))[0]
        with open(path) as file:
            for line in file:
                rs = re.findall('([0-9]+\.[0-9]+) operations/sec', line)
                if rs:
                    throughput_by_client.append(float(rs[0]))
    throughput_reqsec.append(np.percentile(throughput_by_client,PERCENTILE))

fig = plt.figure()
fig.suptitle('parallel={}, read={}%, conflict={}%'.format(
    parallel, read, conflict))
plt.title('Replica')
plt.xlabel('req/sec')
plt.xticks(rotation=45)
plt.ylabel('latency')
plt.plot(throughput_reqsec, latency, marker="D", label='requests')
k=0
for i,j in zip(throughput_reqsec, latency):
    plt.annotate(str(nclients[k]),  xy=(i, j), textcoords="offset points", xytext=(0,10), ha="center")
    k+=1

fig.tight_layout()
plt.show()
