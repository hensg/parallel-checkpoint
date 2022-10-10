#!/usr/bin/env python

import argparse
import re
import os
from datetime import datetime, timedelta
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
parser.add_argument("--dir", type=str, required=True)

args = parser.parse_args()

Y_MAX = 30000


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
    print("Generating image for: " + prefix)
    print("Datetime: " + datetime_exp)
    try:
        os.mkdir("images")
    except OSError:
        pass
    try:
        os.mkdir("images/name=sobrecarga")
    except OSError:
        pass
    try:
        os.mkdir("images/name=sobrecarga/datetime=" + datetime_exp)
    except OSError:
        pass

    last_checkpoint_datetime = None
    checkpoint_intervals = {}
    for path in Path(args.dir).rglob(prefix + "/server*.log"):
        node = re.findall("server_([0-9]{3}).log", str(path))[0]
        with open(str(path)) as file:
            for line in file:
                if (
                    "Initializing checkpointing procedure" in line
                    or "Checkpointing has finished" in line
                ):
                    dt = re.findall("[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}", line)
                    if node not in checkpoint_intervals:
                        checkpoint_intervals[node] = []

                    cp_datetime = datetime.strptime(dt[0], "%H:%M:%S.%f")
                    checkpoint_intervals[node].append(cp_datetime)
                    last_checkpoint_datetime = cp_datetime

    throughput_datetime = {}
    throughput_reqsec = {}
    for path in Path(args.dir).rglob(prefix + "*throughput_*.log"):
        idle = True
        node = re.findall("throughput_([0-9]{3}).log", str(path))[0]
        with open(str(path)) as file:
            for line in file:
                if "ThroughputStatistics2 - Replica" or "ThroughputStatistics2 - Replica" in line:
                    rs = re.findall(r"(\d+\.?\d*) operations/sec", line)
                    if rs:
                        dt = re.findall("([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3})", line)
                        if node not in throughput_datetime:
                            throughput_datetime[node] = []
                            throughput_reqsec[node] = []

                        dt_through = datetime.strptime(dt[0], "%H:%M:%S.%f")
                        throughput_datetime[node].append(dt_through)
                        throughput_reqsec[node].append(float(rs[0]))

        if node in throughput_datetime:
            throughput_reqsec[node] = np.array(throughput_reqsec[node])
            throughput_datetime[node] = np.array(throughput_datetime[node])

    latency_by_time = {}
    for path in Path(args.dir).rglob(prefix + "/client_latency.log"):
        with open(str(path)) as file:
            for line in file:
                dt = re.findall("([0-9]{2}:[0-9]{2}:[0-9]{2})", line)
                rs = re.findall("([0-9]+)millis", line)
                if rs:
                    log_date = datetime.strptime(dt[0], "%H:%M:%S")
                    latency_ms = int(rs[0])
                    # if last_checkpoint_datetime and ((log_date - timedelta(seconds=15)) < last_checkpoint_datetime):
                    latency_by_time[log_date] = latency_ms

    i = 1
    for node in throughput_datetime:
        print(throughput_reqsec)
        # plt.subplot(2, 2, i)
        fig = plt.figure()
        fig.suptitle(
            "parallel={}, read={}%, conflict={}%, checkpoint={}".format(
                parallel, read, conflict, checkpoint
            )
        )
        # plt.subplot(1, 1, i)
        #plt.subplot(1, 1, 1)
        plt.title("Replica " + str(i))
        plt.xlabel("datetime")
        plt.xticks(rotation=45)
        plt.ylabel("req/sec")
        plt.plot(
            throughput_datetime[node],
            throughput_reqsec[node],
            label="requests",
        )
        # seclocator = matdates.SecondLocator(interval=4)
        ax = plt.gca()
        # ax.xaxis.set_major_locator(seclocator)
        ax.set_ylim([0, Y_MAX])
        i += 1
        cs = []
        if node in checkpoint_intervals:
            for ch in checkpoint_intervals[node]:
                ax.axvline(ch, color="r", linestyle="--", label="checkpointing")
                cs.append(ch)
                if len(cs) == 2:
                    ax.fill_betweenx([0, Y_MAX], cs[0], cs[1], alpha=0.2, color="C1")
                    cs = []

        ax.legend(labels=["requests", "checkpointing"], loc="upper right")

        fig.tight_layout()
        path = (
            "images/name=sobrecarga"
            + "/datetime="
            + datetime_exp
            + "/read_"
            + read
            + "_node_"
            + node
            + "_conflict_"
            + conflict
            + "_threads_"
            + threads
            + "_checkpoint_"
            + checkpoint
            + "_parallel_"
            + parallel
            + ".png"
        )
        plt.savefig(
            path,
            dpi=355,
        )
        plt.show()
        print(path)
        plt.close()

    fig = plt.figure()
    fig.suptitle(
        "parallel={}, read={}%, conflict={}%, checkpoint={}".format(
            parallel, read, conflict, checkpoint
        )
    )

    i = 1
    for node in throughput_datetime:
        # plt.subplot(2, 2, i)
        # plt.subplot(1, 1, i)
        plt.title("Replica " + str(i))
        plt.xlabel("datetime")
        plt.xticks(rotation=45)
        plt.ylabel("latency (millis)")
        plt.plot(
            latency_by_time.keys(),
            latency_by_time.values(),
            "g",
            label="requests",
        )
        ax = plt.gca()
        ax.set_ylim([0, 120])
        i += 1
        cs = []
        if node in checkpoint_intervals:
            for ch in checkpoint_intervals[node]:
                ax.axvline(ch, color="r", linestyle="--", label="checkpointing")
                cs.append(ch)
                if len(cs) == 2:
                    ax.fill_betweenx([0, Y_MAX], cs[0], cs[1], alpha=0.2, color="C1")
                    cs = []

        ax.legend(labels=["latency", "checkpointing"], loc="upper right")

    fig.tight_layout()
    plt.savefig(
        "images/name=sobrecarga"
        + "/datetime="
        + datetime_exp
        + "/read_"
        + read
        + "_conflict_"
        + conflict
        + "_threads_"
        + threads
        + "_checkpoint_"
        + checkpoint
        + "_parallel_"
        + parallel
        + "_latency"
        + ".png",
        dpi=355,
    )
    plt.show()
    plt.close()


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
    dt = re.findall("datetime=([0-9]+-[0-9]+-[0-9]+_[0-9]+-[0-9]+-[0-9]+)", str(path))[
        0
    ]

    _generate(parallel, read, conflict, run, threads, checkpoint, dt)
    print("Finished")
