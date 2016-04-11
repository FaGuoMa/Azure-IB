#Z score function prototype

import pandas as pd
import numpy as np

##import data from the csv

#change this path for your machine
csv_path = "C:\\Users\\treyd_000\\Desktop\\Quantinsti\\Project\\CL1 Comdty.csv"

mktdata = pd.DataFrame.from_csv(csv_path)

  # parameter initializations
n_sma = 30
n_stdev = 30
z_threshold = 2
z_close_thresh = .2

# rolling calculations
mktdata["sma"] = mktdata["close"].rolling(window=n_sma).mean()
mktdata["stdev"] = mktdata["close"].rolling(window=n_stdev).std()
mktdata["zscore"] = (mktdata.close - mktdata.sma) / mktdata.stdev


#create empty columns to store state and signal
mktdata["state"] = "FLAT"
mktdata["signal"] = "NONE"

# clean df
mktdata.dropna(axis=0, how="any", inplace=True)
mktdata.reset_index(drop=True)


# for loop to execute state and signal
for i in range(0, len(mktdata)-1):

    # Enter Long Position Signal
    if (mktdata.loc[i, "zscore"] > z_threshold) and (mktdata.loc[i, "zscore"] <= z_threshold):
        mktdata.loc[i, "signal"] = "BOT"
        mktdata.loc[i, "state"] = "LONG"
    # Enter Short Position Signal
    elif (mktdata.loc[i, "zscore"] < -z_threshold) and (mktdata.loc[i, "zscore"] >= -z_threshold):
        mktdata.loc[i, "signal"] = "SLD"
        mktdata.loc[i, "state"] ="SHORT"
    # Close Long position
    elif mktdata.loc[i-1, "state"] == "LONG" and mktdata.loc[i, "zscore"] <= z_close_thresh:
        mktdata.loc[i, "signal"] = "SLD"
        mktdata.loc[i, "state"] = "FLAT"
    # Close Short Position
    elif mktdata.loc[i - 1, "state"] == "SHORT" and mktdata.loc[i, "zscore"] >= -z_close_thresh:
        mktdata.loc[i, "signal"] = "BOT"
        mktdata.loc[i, "state"] = "FLAT"
    # no signals carry state
    else:
        mktdata.loc[i, "signal"] = "NONE"
        mktdata.loc[i, "state"] = mktdata.loc[i-1, "state"]
    print i/(len(mktdata)-1)


print(mktdata)
