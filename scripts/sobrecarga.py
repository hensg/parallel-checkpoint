#!/usr/bin/env python

import argparse
import re
import os
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


PERCENTILE=95

def _generate(parallel, read, conflict, run, threads, checkpoint):
    prefix = ('**/checkpoint='+checkpoint+ '/server_threads='+threads+'/**/partitioned='+
        parallel+'/**/read='+read+'/conflict='+conflict+'/')
    print("Generating image for: " + prefix)

    latency = []
    latency_datetime = []
    for path in Path(args.dir).rglob(prefix + '/client.log'):
        with open(path) as file:
            lats = []
            for line in file:
                dt = re.findall('[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}', line)
                rs = re.findall('95th percentile for [0-9]+ executions = ([0-9]+) us', line)
                if rs:
                    latency.append(int(rs[0])/1000)
                    latency_datetime.append(datetime.strptime(dt[0], '%H:%M:%S.%f'))
                    #lats.append(int(rs[0])/1000)
        #latency.append(np.percentile(lats,PERCENTILE))

    checkpoint_intervals = {}
    for path in Path(args.dir).rglob(prefix+'/server*.log'):
        node = re.findall('server_([0-9]{3}).log', str(path))[0]
        with open(path) as file:
            for line in file:
                if 'Initializing checkpointing procedure' in line or 'Checkpointing has finished' in line:
                    dt = re.findall('[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}', line)
                    if node not in checkpoint_intervals:
                        checkpoint_intervals[node] = []
                    checkpoint_intervals[node].append(
                        datetime.strptime(dt[0], '%H:%M:%S.%f'))

    throughput_datetime = {}
    throughput_reqsec = {}
    for path in Path(args.dir).rglob(prefix+'*throughput_*.log'):
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
                            throughput_reqsec[node] = []
                        if float(rs[0]) > 0:
                            idle = False
                        if not idle:
                            throughput_datetime[node].append(
                                datetime.strptime(dt[0], '%H:%M:%S.%f'))
                            throughput_reqsec[node].append(float(rs[0]))
        throughput_datetime[node] = np.array(throughput_datetime[node])
        throughput_reqsec[node] = np.array(throughput_reqsec[node])


    Y_MAX =50000
    fig = plt.figure()
    fig.suptitle('parallel={}, read={}%, conflict={}%'.format(
        parallel, read, conflict))
    i = 1
    for node in checkpoint_intervals:
        #plt.subplot(2, 2, i)
        plt.subplot(1, 1, i)
        plt.title('Replica ' + str(i))
        plt.xlabel('datetime')
        plt.xticks(rotation=45)
        plt.ylabel('req/sec')
        plt.plot(throughput_datetime[node],
                 throughput_reqsec[node], label='requests')
        #seclocator = matdates.SecondLocator(interval=4)
        ax = plt.gca()
        #ax.xaxis.set_major_locator(seclocator)
        ax.set_ylim([0, Y_MAX])
        i += 1
        cs = []
        for ch in checkpoint_intervals[node]:
            ax.axvline(ch, color='r', linestyle='--', label='checkpointing')
            cs.append(ch)
            if len(cs) == 2:
                ax.fill_betweenx([0, Y_MAX], cs[0], cs[1], alpha=0.2, color='C1')
                cs = []

        #ax2 = ax.twinx()
        #color = 'tab:green'
        #ax2.set_ylabel('latency(millis)', color=color)  # we already handled the x-label with ax1
        #ax2.plot(latency_datetime, latency, color=color)
        #ax2.tick_params(axis='y', labelcolor=color)

        ax.legend(labels=['requests', 'checkpointing'], loc='upper right')
        break

    fig.tight_layout()
    plt.savefig('images/name=sobrecarga/read_'+read+'_conflict_'+
                conflict+'_threads_'+threads+'_checkpoint_'+checkpoint+'_parallel_'+parallel+'.png', dpi=355)
    plt.close()

parallel = None
read = None
conflict = None
run = None
checkpoint = None
threads = None

for path in Path(args.dir).rglob('*client.log'):
    parallel = (re.findall('partitioned=(true|false)', str(path))[0])
    read = re.findall('read=([0-9]+)', str(path))[0]
    conflict = (re.findall('conflict=([0-9]+)', str(path))[0])
    run = re.findall('run=([0-9]+)', str(path))[0]
    threads = re.findall('server_threads=([0-9]+)', str(path))[0]
    checkpoint = re.findall('checkpoint=([0-9]+)', str(path))[0]

    try:
        os.mkdir('images/name=sobrecarga/')
    except OSError as error:
        pass

    _generate(parallel, read, conflict, run, threads, checkpoint)
