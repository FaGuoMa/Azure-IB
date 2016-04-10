# -*- coding: utf-8 -*-
"""
Created on Sun Apr  3 12:50:17 2016

@author: maxime_back
"""
import numpy as np
import pandas as pd
import urllib2
import json as json
class MLcall:
    def __init__(self):
        pass
        


#this will fairly easily plug in a interm datastore
    @staticmethod    
    def call_ml(ohlc):
        url = 'https://asiasoutheast.services.azureml.net/workspaces/1012bd9ca9a140d3b79254b1262c7321/services/fb54b0cf57754af6a53afb6de37c811e/execute?api-version=2.0&details=true'
        api_key = 'IkGTbXZUvFe0B/KpPkZ/RWT8B76sD/Nsxg70VguQM9aaJEbeO8f+rx/PMwJy/RwVK1wKO5PsDIxbd57ZV7OGmw==' # Replace this with the API key for the web service
        
        #vectorizing, the first value is the oldest
        intm = ohlc.tail(5)
        ml_set = intm["volume"].tolist()
        ml_set = ml_set + intm["returns"].tolist()
        ml_set = ml_set + intm["sma"].tolist()
        ml_set = ml_set + intm["lma"].tolist()
        ml_set = ml_set + intm["rsi"].tolist()
        ml_set = ml_set + intm["atr"].tolist()
        #padding for discarded indicators
        ml_set = ml_set + [0,0,0,0,0,0,0,0,0,0]#I know it's retarded but otherwise Iget a nparray
        ml_set = ml_set + intm["count"].tolist()
        ml_set = np.append(ml_set,intm["monday"][-1])
        ml_set = np.append(ml_set,intm["roll"][-1])
        ml_set = np.append(ml_set,intm["busy"][-1])
        ml_set = np.append(ml_set,[0,0])


        data =  {
        "Inputs": {

                "input1":
                {
                    "ColumnNames": ["volume1", "volume2", "volume3", "volume4", "volume5", "returns1", "returns2", "returns3", "returns4", "returns5", "sma1", "sma2", "sma3", "sma4", "sma5", "lma1", "lma2", "lma3", "lma4", "lma5", "rsi1", "rsi2", "rsi3", "rsi4", "rsi5", "atr1", "atr2", "atr3", "atr4", "atr5", "skew1", "skew2", "skew3", "skew4", "skew5", "kurt1", "kurt2", "kurt3", "kurt4", "kurt5", "trades1", "trades2", "trades3", "trades4", "trades5", "monday", "roll", "chicago", "flag1", "flag2"],
                    "Values": [ ml_set.tolist() , ]
                },        },
            "GlobalParameters": {
                }
                }
        headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}

        body = str.encode(json.dumps(data))


        req = urllib2.Request(url, body, headers) 

        try:
            response = urllib2.urlopen(req)    
            result = response.read()
            return str(json.loads(result)["Results"]["output1"]["value"]["Values"][0][0])
#            self.flag = self.json_loads_byteified(result)["Results"]["output1"]["value"]["Values"][1]
             
        except urllib2.HTTPError, error:
            print("The request failed with status code: " + str(error.code))
        
            # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
            print(error.info())
        
            print(json.loads(error.read()))  
            
