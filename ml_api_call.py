# -*- coding: utf-8 -*-
"""
Created on Sun Apr  3 12:50:17 2016

@author: maxime_back
"""


import urllib2
# If you are using Python 3+, import urllib instead of urllib2

import json 

#some BS required tohandle unicode. TOput in a staticmethod decorator
def _byteify(data, ignore_dicts = False):
    # if this is a unicode string, return its string representation
    if isinstance(data, unicode):
        return data.encode('utf-8')
    # if this is a list of values, return list of byteified values
    if isinstance(data, list):
        return [ _byteify(item, ignore_dicts=True) for item in data ]
    # if this is a dictionary, return dictionary of byteified keys and values
    # but only if we haven't already byteified it
    if isinstance(data, dict) and not ignore_dicts:
        return {
            _byteify(key, ignore_dicts=True): _byteify(value, ignore_dicts=True)
            for key, value in data.iteritems()
        }
    # if it's anything else, return it in its original form
    return data

def json_loads_byteified(json_text):
    return _byteify(
        json.loads(json_text, object_hook=_byteify),
        ignore_dicts=True
    )

#this will fairly easily plug in a interm datastore

data =  {

        "Inputs": {

                "input1":
                {
                    "ColumnNames": ["volume1", "volume2", "volume3", "volume4", "volume5", "returns1", "returns2", "returns3", "returns4", "returns5", "sma1", "sma2", "sma3", "sma4", "sma5", "lma1", "lma2", "lma3", "lma4", "lma5", "rsi1", "rsi2", "rsi3", "rsi4", "rsi5", "atr1", "atr2", "atr3", "atr4", "atr5", "skew1", "skew2", "skew3", "skew4", "skew5", "kurt1", "kurt2", "kurt3", "kurt4", "kurt5", "trades1", "trades2", "trades3", "trades4", "trades5", "monday", "roll", "chicago", "flag1", "flag2"],
                    "Values": [ [ "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "value", "value" ], [ "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "value", "value" ], ]
                },        },
            "GlobalParameters": {
}
    }

body = str.encode(json.dumps(data))

url = 'https://asiasoutheast.services.azureml.net/workspaces/1012bd9ca9a140d3b79254b1262c7321/services/fb54b0cf57754af6a53afb6de37c811e/execute?api-version=2.0&details=true'
api_key = 'IkGTbXZUvFe0B/KpPkZ/RWT8B76sD/Nsxg70VguQM9aaJEbeO8f+rx/PMwJy/RwVK1wKO5PsDIxbd57ZV7OGmw==' # Replace this with the API key for the web service
headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}

req = urllib2.Request(url, body, headers) 

try:
    response = urllib2.urlopen(req)


    result = response.read()
    print(json_loads_byteified(result)["Results"]["output1"]["value"]["Values"][1])
     
except urllib2.HTTPError, error:
    print("The request failed with status code: " + str(error.code))

    # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
    print(error.info())

    print(json.loads(error.read()))                 