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
nclients = None
for path in Path(args.dir).rglob('*.log'):
    nclients = re.findall('clients=([0-9]+)', str(path))[0]
    parallel = re.findall('partitioned=(true|false)', str(path))[0]
    read = re.findall('read=([0-9]+)', str(path))[0]
    conflict = re.findall('conflict=([0-9]+)', str(path))[0]
    run = re.findall('run=([0-9]+)', str(path))[0]
    break

latency = []
for path in Path(args.dir).rglob('client.log'):
    with open(path) as file:
        for line in file:
            rs = re.findall('99th percentile for [0-9]+ executions = ([0-9]+) us', line)
            if rs:
                lat = int(rs[0])
    latency.append(lat)

throughput_datetime = {}
throughput_reqsec = {}
for path in Path(args.dir).rglob('throughput_*.log'):
    idle = True
    node = re.findall('throughput_([0-9]{3}).log', str(path))[0]
    with open(path) as file:
        for line in file:
            if 'ThroughputStatistics - Replica' in line:
                rs = re.findall('([0-9]+\.[0-9]+) operations/sec', line)
                if rs:
                    dt = re.findall(
                        '([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3})', line)
                    if node not in throughput_datetime:
                        throughput_datetime[node] = []
                    if node not in throughput_reqsec:
                        throughput_reqsec[node] = []
                    if float(rs[0]) > 0:
                        idle = False
                    if not idle:
                        throughput_datetime[node].append(
                            datetime.strptime(dt[0], '%H:%M:%S.%f'))
                        throughput_reqsec[node].append(float(rs[0]))
    throughput_datetime[node] = np.array(throughput_datetime[node])
    throughput_reqsec[node] = np.array(throughput_reqsec[node])


Y_MAX = 5000
fig = plt.figure()
fig.suptitle('parallel={}, read={}%, conflict={}%'.format(
    parallel, read, conflict))
i = 1
for node in throughput_reqsec:
    avg_throughput = round(sum(throughput_reqsec[node])/len(throughput_reqsec[node]))
    print(avg_throughput)

    plt.subplot(2, 2, i)
    plt.title('Replica ' + str(i))
    plt.xlabel('req/sec')
    plt.xticks(rotation=45)
    plt.ylabel('latency')
    plt.plot(avg_throughput, latency[0], label='requests')

fig.tight_layout()
plt.show()
