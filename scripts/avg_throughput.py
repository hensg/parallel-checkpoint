#!/usr/bin/env python

import argparse
import re
import os
from datetime import datetime
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser()
parser.add_argument("--dir", type=str, required=True)

args = parser.parse_args()


parallel = None
read = None
conflict = None
run = None
checkpoint = None
threads = None
#    dt = re.findall("datetime=([0-9]+-[0-9]+-[0-9]+_[0-9]+-[0-9]+-[0-9]+)", str(path))[
#        0
#    ]
#    try:
#        os.mkdir("images/name=sobrecarga/datetime=" + dt)
#    except OSError:
#        pass

throughput_reqsec = {}

for path in Path(args.dir).rglob("**/**/*throughput_*.log"):
    # print(f"Processing path {path}")
    parallel = re.findall("partitioned=(true|false)", str(path))[0]
    read = re.findall("read=([0-9]+)", str(path))[0]
    conflict = re.findall("conflict=([0-9]+)", str(path))[0]
    run = re.findall("run=([0-9]+)", str(path))[0]
    threads = re.findall("server_threads=([0-9]+)", str(path))[0]
    checkpoint = re.findall("checkpoint=([0-9]+)", str(path))[0]

    idle = True
    node = re.findall("throughput_([0-9]{3}).log", str(path))[0]

    with open(path) as file:
        for line in file:
            if "ThroughputStatistics - Replica" in line:
                if parallel not in throughput_reqsec:
                    throughput_reqsec[parallel] = {}
                if threads not in throughput_reqsec[parallel]:
                    throughput_reqsec[parallel][threads] = {}
                if checkpoint not in throughput_reqsec[parallel][threads]:
                    throughput_reqsec[parallel][threads][checkpoint] = []
                rs = re.findall("([0-9]+\.[0-9]+) operations/sec", line)
                if rs:
                    if float(rs[0]) > 1000:
                        idle = False
                    if not idle:
                        (
                            throughput_reqsec[parallel][threads][checkpoint].append(
                                (float(rs[0]))
                            )
                        )


labels = ["400k", "600k", "800k", "NO_CHECKPOINT"]
width = 0.10
x = np.arange(len(labels))
fig, ax = plt.subplots()

cut_out = 3
i = -3

for parallel in throughput_reqsec:
    for threads in throughput_reqsec[parallel]:
        i = i + 1
        print(throughput_reqsec[parallel][threads][checkpoint][:-cut_out])
        avgs = [
            np.average(
                throughput_reqsec[parallel][threads][checkpoint][:-cut_out]
            )
            for checkpoint in throughput_reqsec[parallel][threads]
        ]

        label = f"{threads}p" if parallel == 'true' else f"{threads}"
        ax.bar(
            x + i * width,
            height=avgs,
            width=width,
            label=label,
        )


ax.set_ylabel("Avg throughput (req/sec)")
ax.set_xlabel("Checkpoint intervals (1.5M requests)")
ax.set_title("Throughput vs Checkpoint intervals (1.5M requests)")
ax.set_xticks(x, labels)
ax.legend()

fig.tight_layout()
plt.show()
