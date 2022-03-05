#!/usr/bin/env python
import argparse
import os
import re
from datetime import datetime
from pathlib import Path

import matplotlib as mpl
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

def _generate(parallel, read, conflict, run, threads, checkpoint, datetime_exp):
    prefix = (
        "**/checkpoint="
        + checkpoint
        + "/server_threads="
        + threads
        + "/**/partitioned="
        + parallel
        + "/**/read="
        + read
        + "/conflict="
        + conflict
        + "/"
    )
    checkpoint_intervals = {}
    start = {}
    paths = Path(args.dir).rglob(prefix + 'server_*.log')
    
    for path in paths:
        print(path)
        node = re.findall('server_([0-9]{3}).log', str(path))[0]

        with open(path) as file:
            for line in file:
                dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Requesting checkpoint state to .* of partition ([0-9]+)', line)
                if dt:
                    if node not in checkpoint_intervals:
                        checkpoint_intervals[node] = {}
                    if dt[0][1] not in checkpoint_intervals[node]:
                        checkpoint_intervals[node][(dt[0][1])] = {}
                    start[dt[0][1]] = datetime.strptime(dt[0][0], '%H:%M:%S.%f')
                    checkpoint_intervals[node][(dt[0][1])]["request-checkpoint"] = dt[0][0]

                
                dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Received the checkpoint of partition ([0-9]+) with size ([0-9]+)', line)
                if dt:
                    checkpoint_intervals[node][dt[0][1]]["received-checkpoint"] = dt[0][0]
                    start[dt[0][1]] = datetime.strptime(dt[0][0], '%H:%M:%S.%f')

                dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Scheduling checkpoint installation of partition ([0-9]+)', line)
                if dt:
                    checkpoint_intervals[node][dt[0][1]]["scheduling-checkpoint-installation"] = dt[0][0]
                    start[dt[0][1]] = datetime.strptime(dt[0][0], '%H:%M:%S.%f')

                dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Installing snapshot of partition ([0-9]+)', line)
                if dt:
                    checkpoint_intervals[node][dt[0][1]]["installing-checkpoint"] = dt[0][0]
                    start[dt[0][1]] = datetime.strptime(dt[0][0], '%H:%M:%S.%f')

                dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Snapshot of partition ([0-9]+) installed', line)
                if dt:
                    checkpoint_intervals[node][dt[0][1]]["checkpoint-installed"] = dt[0][0]
                    start[dt[0][1]] = datetime.strptime(dt[0][0], '%H:%M:%S.%f')
                
                dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Error installing snapshot of partition ([0-9]+)', line)
                if dt:
                    checkpoint_intervals[node][dt[0][1]]["error-installing-checkpoint"] = dt[0][0]
                    start[dt[0][1]] = datetime.strptime(dt[0][0], '%H:%M:%S.%f')

                dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Scheduling log installation of partition ([0-9]+)', line)
                if dt:
                    checkpoint_intervals[node][dt[0][1]]["scheduling-logs-installation"] = dt[0][0]
                    start[dt[0][1]] = datetime.strptime(dt[0][0], '%H:%M:%S.%f')

                dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Requesting log of partition ([0-9]+)', line)
                if dt:
                    checkpoint_intervals[node][dt[0][1]]["requesting-logs"] = dt[0][0]
                    start[dt[0][1]] = datetime.strptime(dt[0][0], '%H:%M:%S.%f')
                
                dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Installing logs of partition ([0-9]+)', line)
                if dt:
                    checkpoint_intervals[node][dt[0][1]]["installing-logs"] = dt[0][0]
                    start[dt[0][1]] = datetime.strptime(dt[0][0], '%H:%M:%S.%f')

                dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Error installing logs of partition ([0-9]+)', line)
                if dt:
                    checkpoint_intervals[node][dt[0][1]]["error-installing-logs"] = dt[0][0]
                    start[dt[0][1]] = datetime.strptime(dt[0][0], '%H:%M:%S.%f')

                dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Logs of partition ([0-9]+) installed', line)
                if dt:
                    checkpoint_intervals[node][dt[0][1]]["logs-installed"] = dt[0][0]
                    start[dt[0][1]] = datetime.strptime(dt[0][0], '%H:%M:%S.%f')

                dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Recovery finished for partition ([0-9]+)', line)
                if dt:
                    checkpoint_intervals[node][dt[0][1]]["recovery-finished"] = dt[0][0]
                    start[dt[0][1]] = datetime.strptime(dt[0][0], '%H:%M:%S.%f')

    import json
    print(json.dumps(checkpoint_intervals, indent=2))


    # category_names = ['Requesting checkpoint', 'Installing checkpoint', 'Installing logs']
    # for node in checkpoint_intervals:
    #     labels = []        
    #     if parallel == 'true':
    #         labels = ['Partition ' + x[0] for x in checkpoint_intervals[node]]
    #     else:
    #         labels = ['Single Partition']

    #     N = 4
    #     menMeans = (20, 35, 30, 35, -27)
    #     womenMeans = (25, 32, 34, 20, -25)
    #     menStd = (2, 3, 4, 1, 2)
    #     womenStd = (3, 5, 2, 3, 3)
    #     ind = np.arange(N)    # the x locations for the groups

    #     width = 0.35       # the width of the bars: can also be len(x) sequence
    #     fig, ax = plt.subplots()

    #     p1 = ax.bar(ind, menMeans, width, yerr=menStd, label='Men')
    #     p2 = ax.bar(ind, womenMeans, width,
    #                 bottom=menMeans, yerr=womenStd, label='Women')

    #     ax.axhline(0, color='grey', linewidth=0.8)
    #     ax.set_ylabel('Scores')
    #     ax.set_title('Scores by group and gender')
    #     ax.set_xticks(ind, labels=['p1', 'p2', 'p3', 'p4'])
    #     ax.legend()

    #     # Label with label_type 'center' instead of the default 'edge'
    #     ax.bar_label(p1, label_type='center')
    #     ax.bar_label(p2, label_type='center')
    #     ax.bar_label(p2)
    #     plt.show()

    #     data = np.array(list([x[1] for x in tup]))
    #     data_cum = data.cumsum(axis=0)
    #     category_colors = plt.colormaps['RdYlBu'](
    #         np.linspace(0.15, 0.85, data.shape[0]))
        
    #     fig = plt.figure()
    #     fig.suptitle(t='parallel={}, read={}%, conflict={}%'.format(parallel, read, conflict))
    #     ax = plt.gca()
    #     ax.invert_yaxis()
    #     ax.xaxis.set_visible(True)
    #     ax.set_xlim(0, np.sum(data, axis=0).max())
    #     ax.set_xlabel('seconds')
    #     ax.set_xticks(np.arange(0,10,1))

    #     for i, (colname, color) in enumerate(zip(category_names, category_colors)):
    #         widths = data[:, i]
    #         starts = data_cum[:, i] - widths
    #         rects = ax.barh(labels, widths, left=starts, height=0.8,
    #                         label=colname, color=color)

    #         r, g, b, _ = color
    #         text_color = 'white' if r * g * b < 0.5 else 'darkgrey'
    #         ax.bar_label(rects, label_type='center', color=text_color)
    #     ax.legend(ncol=len(category_names), bbox_to_anchor=(0, 1),
    #             loc='lower left', fontsize='small')
        
    #     break


    # fig.tight_layout()
    # plt.savefig(
    #     "images/name=recovery"
    #     + "/datetime=" + datetime_exp
    #     + "/read_"
    #     + read
    #     + "_conflict_"
    #     + conflict
    #     + "_threads_"
    #     + threads
    #     + "_checkpoint_"
    #     + checkpoint
    #     + "_parallel_"
    #     + parallel
    #     + ".png",
    #     dpi=355,
    # )
    # plt.close()


parallel = None
read = None
conflict = None
run = None
checkpoint = None
threads = None

for path in Path(args.dir).rglob("*client.log"):
    parallel = re.findall("partitioned=(true|false)", str(path))[0]
    read = re.findall("read=([0-9]+)", str(path))[0]
    conflict = re.findall("conflict=([0-9]+)", str(path))[0]
    run = re.findall("run=([0-9]+)", str(path))[0]
    threads = re.findall("server_threads=([0-9]+)", str(path))[0]
    checkpoint = re.findall("checkpoint=([0-9]+)", str(path))[0]
    dt = re.findall("datetime=([0-9]+-[0-9]+-[0-9]+_[0-9]+-[0-9]+-[0-9]+)", str(path))[0]

    try:
        dir = "images/name=recovery/datetime="+dt
        os.mkdir("images/name=recovery")
        os.mkdir(dir)
    except OSError as error:
        pass

    _generate(parallel, read, conflict, run, threads, checkpoint, dt)
