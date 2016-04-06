"""
Data Parser for recorded IB data

Author Derek Wong

Date: 2016-04-05
"""

import pandas as pd
import time

min_src_path = "C:\\Users\\treyd_000\\Desktop\\Quantinsti\\Azure-IB\\Azure-IB\\ohlc.csv"
tick_src_path = "C:\\Users\\treyd_000\\Desktop\\Quantinsti\\Azure-IB\\Azure-IB\\data.csv"

class DataParser:

    def __init__(self):
        pass

    def min_data_to_panda(self, src_path):
        start_time = time.clock()
        raw = pd.read_csv(src_path, sep=",",
                          names=["Datetime", "Open", "High", "Low", "Close", "Volume", "Count"],
                          skiprows=1)
        index = pd.to_datetime(raw.Datetime)
        raw.index = index
        clean = raw.drop(["Datetime"], 1)
        end_time = time.clock()
        print("Import Complete. ", clean.shape[0], " Lines Read in {:.2f}".format(end_time - start_time), "sec")
        return clean

    def tick_data_to_panda(self, src_path, padding = True):
        start_time = time.clock()
        raw = pd.read_csv(src_path, sep=",",
                          names=["Datetime", "Price", "Size", "Ask_price", "Ask_size", "Bid_price", "Bid_size"],
                          skiprows=1)
        index = pd.to_datetime(raw.Datetime)
        raw.index = index
        clean = raw.drop(["Datetime"], 1)
        if padding == True:
           clean.fillna(method="pad", inplace=True)
            clean.dropna(axis=0, inplace=True)
        end_time = time.clock()
        print("Import Complete. ", clean.shape[0], " Lines Read in {:.2f}".format(end_time - start_time), "sec")
        return clean