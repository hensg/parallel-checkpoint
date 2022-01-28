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
import matplotlib as mpl

parser = argparse.ArgumentParser()
parser.add_argument('--dir', type=str, required=True)

args = parser.parse_args()

parallel = None
read = None
conflict = None
run = None
for path in Path(args.dir).rglob('*.log'):
    parallel = re.findall('partitioned=(true|false)', str(path))[0]
    read = re.findall('read=([0-9]+)', str(path))[0]
    conflict = re.findall('conflict=([0-9]+)', str(path))[0]
    run = re.findall('run=([0-9]+)', str(path))[0]
    break

checkpoint_intervals = {}
start = {}
for path in Path(args.dir).rglob('server*.log'):
    print(path)
    node = re.findall('server_([0-9]{3}).log', str(path))[0]

    with open(path) as file:
        for line in file:
            dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Requesting checkpoint state to .* of partition ([0-9]+)', line)
            if dt:
                if node not in checkpoint_intervals:
                    checkpoint_intervals[node] = {}
                if dt[0][1] not in checkpoint_intervals[node]:
                    checkpoint_intervals[node][(dt[0][1])] = []
                start[dt[0][1]] = datetime.strptime(dt[0][0], '%H:%M:%S.%f')

            
            dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Received the checkpoint of partition ([0-9]+) with size ([0-9]+)', line)
            if dt:
                checkpoint_intervals[node][dt[0][1]].append((datetime.strptime(dt[0][0], '%H:%M:%S.%f') - start[dt[0][1]]).total_seconds())
                start[dt[0][1]] = datetime.strptime(dt[0][0], '%H:%M:%S.%f')

            # dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Installing snapshot of partition ([0-9]+)', line)
            # if dt:
            #     checkpoint_intervals[node][dt[0][1]].append((datetime.strptime(dt[0][0], '%H:%M:%S.%f') - start[dt[0][1]]).total_seconds())
            #     start[dt[0][1]] = datetime.strptime(dt[0][0], '%H:%M:%S.%f')

            dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Snapshot of partition ([0-9]+) installed', line)
            if dt:
                checkpoint_intervals[node][dt[0][1]].append((datetime.strptime(dt[0][0], '%H:%M:%S.%f') - start[dt[0][1]]).total_seconds())
                start[dt[0][1]] = datetime.strptime(dt[0][0], '%H:%M:%S.%f')

            #     dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Requesting log of partition ([0-9]+)', line)
            #     checkpoint_intervals[node][dt[0][1]].append((datetime.strptime(dt[0][0], '%H:%M:%S.%f') - start).total_seconds())
            #     start = datetime.strptime(dt[0][0], '%H:%M:%S.%f')

            dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Log of partition ([0-9]+) installed', line)
            if dt:
                checkpoint_intervals[node][dt[0][1]].append((datetime.strptime(dt[0][0], '%H:%M:%S.%f') - start[dt[0][1]]).total_seconds())



category_names = ['Requesting checkpoint', 'Installing checkpoint', 'Installing logs']
for node in checkpoint_intervals:
    labels = []
    tup = []
    for k in checkpoint_intervals[node]:
        if len(checkpoint_intervals[node][k]) > 0:
            tup.append((k, checkpoint_intervals[node][k]))
    print(tup)
    tup.sort(key=lambda x:x[0])
    if parallel == 'true':
        labels = ['Partition ' + x[0] for x in tup]
    else:
        labels = ['Single Partition']

    data = np.array(list([x[1] for x in tup]))
    data_cum = data.cumsum(axis=1)
    category_colors = plt.colormaps['RdYlBu'](
        np.linspace(0.15, 0.85, data.shape[1]))
    
    fig, ax = plt.subplots(figsize=(9.2, 5))
    fig.suptitle(t='parallel={}, read={}%, conflict={}%'.format(parallel, read, conflict))
    ax.invert_yaxis()
    ax.xaxis.set_visible(True)
    ax.set_xlim(0, np.sum(data, axis=1).max())
    ax.set_xlabel('seconds')
    ax.set_xticks(np.arange(0,10,1))

    for i, (colname, color) in enumerate(zip(category_names, category_colors)):
        widths = data[:, i]
        starts = data_cum[:, i] - widths
        rects = ax.barh(labels, widths, left=starts, height=0.8,
                        label=colname, color=color)

        r, g, b, _ = color
        text_color = 'white' if r * g * b < 0.5 else 'darkgrey'
        ax.bar_label(rects, label_type='center', color=text_color)
    ax.legend(ncol=len(category_names), bbox_to_anchor=(0, 1),
              loc='lower left', fontsize='small')


plt.show()