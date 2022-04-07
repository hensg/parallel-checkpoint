#!/usr/bin/env python
import argparse
import re
import os
from datetime import datetime
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt

from bokeh.io import export_png
from bokeh.models import ColumnDataSource, FactorRange
from bokeh.plotting import figure, output_file, save, show


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

    if run == "1":
        continue

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
            if g and datetime.strptime(g[0][0], TIMEFORMAT) < request_checkpoint_datetime:
                request_checkpoint_datetime = datetime.strptime(g[0][0], TIMEFORMAT)

            g = re.findall(
                "(\d+-\d+-\d+ [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Received the checkpoint of partition ([0-9]+) with size ([0-9]+)",
                line,
            )
            if g and (
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
            if g and datetime.strptime(g[0][0], TIMEFORMAT) < request_logs_datetime:
                request_logs_datetime = datetime.strptime(g[0][0], TIMEFORMAT)

            g = re.findall(
                "(\d+-\d+-\d+ [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Received log of partition ([0-9]+) with ([0-9]+) operations",
                line,
            )
            if g and datetime.strptime(g[0][0], TIMEFORMAT) > received_logs_datetime:
                received_logs_datetime = datetime.strptime(g[0][0], TIMEFORMAT)

            g = re.findall(
                "(\d+-\d+-\d+ [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).* Installing snapshot of partition ([0-9]+)",
                line,
            )
            if g and datetime.strptime(g[0][0], TIMEFORMAT) < start_install_cp_datetime:
                start_install_cp_datetime = datetime.strptime(g[0][0], TIMEFORMAT)

            g = re.findall(
                "(\d+-\d+-\d+ [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).* Snapshot of partition ([0-9]+) installed",
                line,
            )
            if g and datetime.strptime(g[0][0], TIMEFORMAT) > finish_install_cp_datetime:
                finish_install_cp_datetime = datetime.strptime(g[0][0], TIMEFORMAT)

            g = re.findall(
                "(\d+-\d+-\d+ [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Installing logs of partition ([0-9]+)",
                line,
            )
            if g and datetime.strptime(g[0][0], TIMEFORMAT) < start_install_logs_datetime:
                start_install_logs_datetime = datetime.strptime(g[0][0], TIMEFORMAT)

            g = re.findall(
                "(\d+-\d+-\d+ [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}).*Logs of partition ([0-9]+) installed",
                line,
            )
            if g and (
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
    rr_tt_logs = (received_logs_datetime - received_checkpoint_datetime).total_seconds()
    print(f"Time taken to request/response logs: {rr_tt_logs}")

    print(f"Starting to install checkpoint at: {start_install_cp_datetime}")
    print(f"Finished installing the checkpoint at: {finish_install_cp_datetime}")
    install_tt_cp = (
        finish_install_cp_datetime - received_logs_datetime
    ).total_seconds()
    print(f"Time taken to install checkpoint: {install_tt_cp}")

    print(f"Starting to install logs at: {start_install_logs_datetime}")
    print(f"Finished installing the log at: {finish_install_logs_datetime}")
    tt_logs = (
        finish_install_logs_datetime - finish_install_cp_datetime
    ).total_seconds()
    print(f"Time taken to install logs: {tt_logs}")

    request_response_tt_cp[parallel][threads][checkpoint].append(rr_tt_cp)
    install_snapshot_tt[parallel][threads][checkpoint].append(install_tt_cp)
    install_logs_tt[parallel][threads][checkpoint].append(tt_logs)
    req_res_logs[parallel][threads][checkpoint].append(rr_tt_logs)


from bokeh.io import output_file, show
from bokeh.models import ColumnDataSource, FactorRange
from bokeh.plotting import figure

factors = [
    ("400k", "non-p"), ("400k", "4"), ("400k", "8"), ("400k", "16"),
    ("800k", "non-p"), ("800k", "4"), ("800k", "8"), ("800k", "16"),
    ("1600k", "non-p"), ("1600k", "4"), ("1600k", "8"), ("1600k", "16"),
]

regions = ['requestingCP', 'installingCP', "requestingLogs", "installingLogs"]

requesting_cp=[
    request_response_tt_cp["false"]["8"]["400000"][0],
    request_response_tt_cp["true"]["4"]["400000"][0],
    request_response_tt_cp["true"]["8"]["400000"][0],
    request_response_tt_cp["true"]["16"]["400000"][0],

    request_response_tt_cp["false"]["8"]["800000"][0],
    request_response_tt_cp["true"]["4"]["800000"][0],
    request_response_tt_cp["true"]["8"]["800000"][0],
    request_response_tt_cp["true"]["16"]["800000"][0],

    request_response_tt_cp["false"]["8"]["1600000"][0],
    request_response_tt_cp["true"]["4"]["1600000"][0],
    request_response_tt_cp["true"]["8"]["1600000"][0],
    request_response_tt_cp["true"]["16"]["1600000"][0],
]

installing_cp=[
    install_snapshot_tt["false"]["8"]["400000"][0],
    install_snapshot_tt["true"]["4"]["400000"][0],
    install_snapshot_tt["true"]["8"]["400000"][0],
    install_snapshot_tt["true"]["16"]["400000"][0],

    install_snapshot_tt["false"]["8"]["800000"][0],
    install_snapshot_tt["true"]["4"]["800000"][0],
    install_snapshot_tt["true"]["8"]["800000"][0],
    install_snapshot_tt["true"]["16"]["800000"][0],

    install_snapshot_tt["false"]["8"]["1600000"][0],
    install_snapshot_tt["true"]["4"]["1600000"][0],
    install_snapshot_tt["true"]["8"]["1600000"][0],
    install_snapshot_tt["true"]["16"]["1600000"][0],
]

requesting_logs=[
    req_res_logs["false"]["8"]["400000"][0],
    req_res_logs["true"]["4"]["400000"][0],
    req_res_logs["true"]["8"]["400000"][0],
    req_res_logs["true"]["16"]["400000"][0],

    req_res_logs["false"]["8"]["800000"][0],
    req_res_logs["true"]["4"]["800000"][0],
    req_res_logs["true"]["8"]["800000"][0],
    req_res_logs["true"]["16"]["800000"][0],


    req_res_logs["false"]["8"]["1600000"][0],
    req_res_logs["true"]["4"]["1600000"][0],
    req_res_logs["true"]["8"]["1600000"][0],
    req_res_logs["true"]["16"]["1600000"][0],
]

installing_logs=[
    install_logs_tt["false"]["8"]["400000"][0],
    install_logs_tt["true"]["4"]["400000"][0],
    install_logs_tt["true"]["8"]["400000"][0],
    install_logs_tt["true"]["16"]["400000"][0],

    install_logs_tt["false"]["8"]["800000"][0],
    install_logs_tt["true"]["4"]["800000"][0],
    install_logs_tt["true"]["8"]["800000"][0],
    install_logs_tt["true"]["16"]["800000"][0],


    install_logs_tt["false"]["8"]["1600000"][0],
    install_logs_tt["true"]["4"]["1600000"][0],
    install_logs_tt["true"]["8"]["1600000"][0],
    install_logs_tt["true"]["16"]["1600000"][0],
]


source = ColumnDataSource(data=dict(
    x=factors,
    requestingCP=requesting_cp,
    installingCP=installing_cp,
    requestingLogs=requesting_logs,
    installingLogs=installing_logs,
))

p = figure(title="Recovery", x_range=FactorRange(*factors), height=800, width=1200,
           toolbar_location=None, tools="")

p.vbar_stack(regions, x='x', width=0.9, alpha=0.5, color=["blue", "red", "green", "orange"], source=source,
             legend_label=regions)

p.title.align = "center"
p.title.text_font_style = "bold"
p.title.text_font_size = "26px"
p.y_range.start = 0
p.y_range.end = 12
p.x_range.range_padding = 0.1
p.xaxis.major_label_orientation = 1
p.xgrid.grid_line_color = None
p.legend.location = "top_center"
p.legend.orientation = "horizontal"
p.yaxis.axis_label = "Seconds"
p.xaxis.axis_label = "Checkpoint interval, partition config"

output_file(filename="images/name=recovery.html", title="Static HTML file")
show(p)