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

TIMEFORMAT = "%Y-%m-%d %H:%M:%S.%f"

request_response_tt_cp = {}
install_snapshot_tt = {}
req_res_logs = {}
install_logs_tt = {}
for path in Path(args.dir).rglob("**/**/*server*.log"):
    parallel = re.findall("partitioned=(true|false)", str(path))[0]
    read = re.findall("read=([0-9]+)", str(path))[0]
    conflict = re.findall("conflict=([0-9]+)", str(path))[0]
    run = re.findall("run=([0-9]+)", str(path))[0]
    threads = re.findall("server_threads=([0-9]+)", str(path))[0]
    checkpoint = re.findall("checkpoint=([0-9]+)", str(path))[0]

    idle = True
    node = re.findall("server_([0-9]{3}).log", str(path))[0]
    request_checkpoint_datetime = datetime(2099, 1, 1)
    received_checkpoint_datetime = datetime(1500, 1, 1)
    request_logs_datetime = datetime(2099, 1, 1)
    received_logs_datetime = datetime(1500, 1, 1)
    recovery_finished_datetime = datetime(1500, 1, 1)
    start_install_cp_datetime = datetime(2099, 1, 1)
    finish_install_cp_datetime = datetime(1500, 1, 1)
    start_install_logs_datetime = datetime(2099, 1, 1)
    finish_install_logs_datetime = datetime(1500, 1, 1)

    if parallel not in request_response_tt_cp:
        request_response_tt_cp[parallel] = {}
        install_snapshot_tt[parallel] = {}
        install_logs_tt[parallel] = {}
        req_res_logs[parallel] = {}
    if threads not in request_response_tt_cp[parallel]:
        request_response_tt_cp[parallel][threads] = {}
        install_snapshot_tt[parallel][threads] = {}
        install_logs_tt[parallel][threads] = {}
        req_res_logs[parallel][threads] = {}
    if checkpoint not in request_response_tt_cp[parallel][threads]:
        request_response_tt_cp[parallel][threads][checkpoint] = []
        install_snapshot_tt[parallel][threads][checkpoint] = []
        install_logs_tt[parallel][threads][checkpoint] = []
        req_res_logs[parallel][threads][checkpoint] = []

    print(f"Processing file: {path}")
    with open(path) as file:
        for line in file:
            g = re.findall(
                "(\d+-\d+-\d+ [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Requesting checkpoint state to .* of partition ([0-9]+)",
                line,
            )
            if g:
                if datetime.strptime(g[0][0], TIMEFORMAT) < request_checkpoint_datetime:
                    request_checkpoint_datetime = datetime.strptime(g[0][0], TIMEFORMAT)

            g = re.findall(
                "(\d+-\d+-\d+ [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Received the checkpoint of partition ([0-9]+) with size ([0-9]+)",
                line,
            )
            if g:
                if (
                    datetime.strptime(g[0][0], TIMEFORMAT)
                    > received_checkpoint_datetime
                ):
                    received_checkpoint_datetime = datetime.strptime(
                        g[0][0], TIMEFORMAT
                    )

            g = re.findall(
                "(\d+-\d+-\d+ [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Requesting log of partition ([0-9]+)",
                line,
            )
            if g:
                if datetime.strptime(g[0][0], TIMEFORMAT) < request_logs_datetime:
                    request_logs_datetime = datetime.strptime(g[0][0], TIMEFORMAT)

            g = re.findall(
                "(\d+-\d+-\d+ [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Received log of partition ([0-9]+) with ([0-9]+) operations",
                line,
            )
            if g:
                if datetime.strptime(g[0][0], TIMEFORMAT) > received_logs_datetime:
                    received_logs_datetime = datetime.strptime(g[0][0], TIMEFORMAT)

            g = re.findall(
                "(\d+-\d+-\d+ [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).* Installing snapshot of partition ([0-9]+)",
                line,
            )
            if g:
                if datetime.strptime(g[0][0], TIMEFORMAT) < start_install_cp_datetime:
                    start_install_cp_datetime = datetime.strptime(g[0][0], TIMEFORMAT)

            g = re.findall(
                "(\d+-\d+-\d+ [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).* Snapshot of partition ([0-9]+) installed",
                line,
            )
            if g:
                if datetime.strptime(g[0][0], TIMEFORMAT) > finish_install_cp_datetime:
                    finish_install_cp_datetime = datetime.strptime(g[0][0], TIMEFORMAT)

            g = re.findall(
                "(\d+-\d+-\d+ [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Installing logs of partition ([0-9]+)",
                line,
            )
            if g:
                if datetime.strptime(g[0][0], TIMEFORMAT) < start_install_logs_datetime:
                    start_install_logs_datetime = datetime.strptime(g[0][0], TIMEFORMAT)

            g = re.findall(
                "(\d+-\d+-\d+ [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Logs of partition ([0-9]+) installed",
                line,
            )
            if g:
                if (
                    datetime.strptime(g[0][0], TIMEFORMAT)
                    > finish_install_logs_datetime
                ):
                    finish_install_logs_datetime = datetime.strptime(
                        g[0][0], TIMEFORMAT
                    )

    print(f"Requested checkpoint at: {request_checkpoint_datetime}")
    print(f"Received the checkpoint response at: {received_checkpoint_datetime}")
    rr_tt_cp = (
        received_checkpoint_datetime - request_checkpoint_datetime
    ).total_seconds()
    print(f"Time taken to request/response checkpoint: {rr_tt_cp}")

    print(f"Requested log of partition at: {request_logs_datetime}")
    print(f"Received logs response at: {received_logs_datetime}")
    rr_tt_logs = (received_logs_datetime - request_logs_datetime).total_seconds()
    print(f"Time taken to request/response logs: {rr_tt_logs}")

    print(f"Starting to install checkpoint at: {start_install_cp_datetime}")
    print(f"Finished installing the checkpoint at: {finish_install_cp_datetime}")
    install_tt_cp = (
        finish_install_cp_datetime - start_install_cp_datetime
    ).total_seconds()
    print(f"Time taken to install checkpoint: {install_tt_cp}")

    print(f"Starting to install logs at: {start_install_logs_datetime}")
    print(f"Finished installing the log at: {finish_install_logs_datetime}")
    tt_logs = (
        finish_install_logs_datetime - start_install_logs_datetime
    ).total_seconds()
    print(f"Time taken to install logs: {tt_logs}")

    request_response_tt_cp[parallel][threads][checkpoint].append(rr_tt_cp)
    install_snapshot_tt[parallel][threads][checkpoint].append(install_tt_cp)
    install_logs_tt[parallel][threads][checkpoint].append(tt_logs)
    req_res_logs[parallel][threads][checkpoint].append(rr_tt_logs)

    # print(f"Recovery finished at: {recovery_finished_datetime}")

labels = ["400k", "600k", "800k"]
width = 0.10
x = np.arange(len(labels))
fig, ax = plt.subplots()

cut_out = 10
i = -3

DEFAULT_COLORS = ["b", "g", "r", "c", "m", "y"]

false_parallel_plotted = False
for parallel in request_response_tt_cp:
    for threads in request_response_tt_cp[parallel]:
        if threads not in ["4", "8", "16"]:
            continue
        if false_parallel_plotted and parallel == "false":
            continue

        colors=DEFAULT_COLORS

        i = i + 1
        requesting_tt = [
            request_response_tt_cp[parallel][threads][cp][0]
            for cp in request_response_tt_cp[parallel][threads]
        ]
        installing_tt = [
            install_snapshot_tt[parallel][threads][cp][0]
            for cp in install_snapshot_tt[parallel][threads]
        ]
        logs_req_res = [
            req_res_logs[parallel][threads][cp][0]
            for cp in req_res_logs[parallel][threads]
        ]
        logs_install_tt = [
            install_logs_tt[parallel][threads][cp][0]
            for cp in install_logs_tt[parallel][threads]
        ]

        req_res_label = (
            f"{threads}p-requesting-cp" if parallel == "true" else "requesting-cp"
        )
        installing_label = (
            f"{threads}p-installling-cp" if parallel == "true" else "installing-cp"
        )
        req_res_logs_label = (
            f"{threads}p-requesting-logs" if parallel == "true" else "requesting-logs"
        )
        installing_logs_label = (
            f"{threads}p-installling-logs" if parallel == "true" else "installing-logs"
        )

        # install logs bar
        ax.bar(
            x + i * width,
            height=logs_install_tt,
            width=width,
            label=installing_logs_label,
            align="edge",
            bottom=np.array(installing_tt)
            + np.array(requesting_tt)
            + np.array(logs_req_res),
        )

        # logs request response bar
        ax.bar(
            x + i * width,
            height=logs_req_res,
            width=width,
            label=req_res_logs_label,
            align="edge",
            bottom=np.array(installing_tt) + np.array(requesting_tt),
        )

        # installing CP bar
        ax.bar(
            x + i * width,
            height=installing_tt,
            width=width,
            label=installing_label,
            align="edge",
            bottom=requesting_tt,
        )

        # requesting CP bar
        ax.bar(
            x + i * width,
            height=requesting_tt,
            width=width,
            label=req_res_label,
            align="edge",
        )
        if parallel == "false":
            false_parallel_plotted = True


ax.set_ylabel("Time taken (seconds)")
ax.set_xlabel("Checkpoint interval (1.5M requests)")
ax.set_title("Time taken on checkpointing")
ax.set_xticks(x, labels)
ax.legend()

fig.tight_layout()
plt.show()

# dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Received the checkpoint of partition ([0-9]+) with size ([0-9]+)', line)
# if dt:
#    checkpoint_intervals[node][dt[0][1]]["received-checkpoint"] = dt[0][0]

# dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Scheduling checkpoint installation of partition ([0-9]+)', line)
# if dt:
#    checkpoint_intervals[node][dt[0][1]]["scheduling-checkpoint-installation"] = dt[0][0]

# dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Installing snapshot of partition ([0-9]+)', line)
# if dt:
#    checkpoint_intervals[node][dt[0][1]]["installing-checkpoint"] = dt[0][0]

# dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Snapshot of partition ([0-9]+) installed', line)
# if dt:
#    checkpoint_intervals[node][dt[0][1]]["checkpoint-installed"] = dt[0][0]
#
# dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Error installing snapshot of partition ([0-9]+)', line)
# if dt:
#    checkpoint_intervals[node][dt[0][1]]["error-installing-checkpoint"] = dt[0][0]

# dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Scheduling log installation of partition ([0-9]+)', line)
# if dt:
#    checkpoint_intervals[node][dt[0][1]]["scheduling-logs-installation"] = dt[0][0]

# dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Requesting log of partition ([0-9]+)', line)
# if dt:
#    checkpoint_intervals[node][dt[0][1]]["requesting-logs"] = dt[0][0]
#
# dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Installing logs of partition ([0-9]+)', line)
# if dt:
#    checkpoint_intervals[node][dt[0][1]]["installing-logs"] = dt[0][0]

# dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Error installing logs of partition ([0-9]+)', line)
# if dt:
#    checkpoint_intervals[node][dt[0][1]]["error-installing-logs"] = dt[0][0]

# dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Logs of partition ([0-9]+) installed', line)
# if dt:
#    checkpoint_intervals[node][dt[0][1]]["logs-installed"] = dt[0][0]

# dt = re.findall('.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Recovery finished for partition ([0-9]+)', line)
# if dt:
#    checkpoint_intervals[node][dt[0][1]]["recovery-finished"] = dt[0][0]
