import matplotlib.pyplot as plt
import matplotlib.cbook as cbook

import numpy as np
import pandas as pd

import os

dirname=os.getcwd()
throughputfile=dirname+"/throughput.data"
checkpointfile=dirname+"/server.data"

with cbook.get_sample_data(throughputfile) as file:
    with cbook.get_sample_data(checkpointfile) as file2:
        th = pd.read_csv(file)
        ch = pd.read_csv(file2)
        th.plot("Date", ["Throughput"], subplots=True)
        plt.axvline(x=ch)
        plt.show()
