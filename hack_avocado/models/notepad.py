import pandas as pd
import numpy as np
import datetime as dt
import pickle

rng = pd.date_range('1/1/2011', periods=72, freq='S')
ts = pd.DataFrame(np.random.randn(len(rng)), index=rng)

# print ts
#
# print ts.index.max()-ts.index.min()
# print ts.index.max()-ts.index.min() > dt.timedelta(seconds=12)
# test = dt.datetime(2011,1,1,0,0,28)
#
# print ts[ts.index < test]
#
# print len(ts)
# print len(ts[ts.index < test])

test = pickle.load("/Users/maxime_back/Documents/avocado/prices_f_norm.pickle")

