#Z score function prototype


import pandas as pd
import numpy as np
import os
##import data from the csv

#so os nomrpath helps porting across machines
path_tickfile = os.path.normpath("/Users/maxime_back/Documents/avocado/algos/CL1Comdty.csv")

#change this path for your machine
csv_path = os.path.normpath(path_tickfile)

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
#mktdata.dropna(axis=0, how="any", inplace=True)
mktdata = mktdata.dropna()

mktdata.reset_index(drop=True)


# for loop to execute state and signal
#for i in range(0, len(mktdata)-1):
# Enter Long Position Signal
    
for i in range(0, 10):
    if mktdata.iloc[i]["zscore"] > z_threshold and mktdata.iloc[i]["zscore"] <= z_threshold:
        mktdata.iloc[i]["signal"] = "BOT"
        mktdata.iloc[i]["state"] = "LONG"
    # Enter Short Position Signal
    elif (mktdata.iloc[i]["zscore"] < -z_threshold) and (mktdata.iloc[i]["zscore"] >= -z_threshold):
        mktdata.iloc[i]["signal"] = "SLD"
        mktdata.iloc[i]["state"] ="SHORT"
    # Close Long position
    elif mktdata.iloc[i-1]["state"] == "LONG" and mktdata.iloc[i]["zscore"] <= z_close_thresh:
        mktdata.iloc[i]["signal"] = "SLD"
        mktdata.iloc[i]["state"] = "FLAT"
    # Close Short Position
    elif mktdata.iloc[i - 1]["state"] == "SHORT" and mktdata.iloc[i]["zscore"] >= -z_close_thresh:
        mktdata.iloc[i]["signal"] = "BOT"
        mktdata.iloc[i]["state"] = "FLAT"
    # no signals carry state
    else:
        mktdata.iloc[i]["signal"] = "NONE"
        mktdata.iloc[i]["state"] = mktdata.iloc[i-1]["state"]
    print i/(len(mktdata)-1)


print(mktdata)
