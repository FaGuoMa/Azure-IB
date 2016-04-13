"""
Heavily inspiredfrom Jack Ma "high Frequency", although a lot of things have to go...

"""
import pandas as pd
from ib.opt import ibConnection, message as ib_message_type
from ib.opt import Connection
import datetime as dt
import time
from classes.ib_util import IBUtil
from classes.stock_data import StockData
#import our ML class call (btw, API key in clear = not smart)
from classes.ml_api_call import MLcall
import params.ib_data_types as datatype
#from params.strategy_parameters import StrategyParameters
from algos.ZscoreEventDriven import Zscore
import threading
import sys
import os
import logging
import numpy as np
import talib as ta
from math import sqrt

if os.path.exists(os.path.join(os.path.curdir,"log.txt")):
    os.remove(os.path.join(os.path.curdir,"log.txt"))
logging.basicConfig(filename=os.path.normpath(os.path.join(os.path.curdir,"log.txt")),level=logging.DEBUG, format='%(asctime)s %(message)s')


#this will need be checked



class HFTModel:

    def __init__(self, host='localhost', port=4001,
                 client_id=130, is_use_gateway=False, evaluation_time_secs=20,
                 resample_interval_secs='30s',
                 moving_window_period=dt.timedelta(seconds=60)):
        self.moving_window_period = moving_window_period
        self.ib_util = IBUtil()

        # Store parameters for this model
#        self.strategy_params = StrategyParameters(evaluation_time_secs,
#                                                  resample_interval_secs)

        self.stocks_data = {}  # Dictionary storing StockData objects.REFACTOR
        self.symbols = None  # List of current symbols
        self.account_code = ""
        self.prices = None  # Store last prices in a DataFrame
        self.ohlc = None # I need another store for minute data (I think)
        self.buffer = list()        
        self.trade_qty = 0
        self.order_id = 0
        self.lock = threading.Lock()
        #addition for hdf store
        self.data_path = os.path.normpath(os.path.join(os.path.curdir,"data.csv"))
        self.ohlc_path = os.path.normpath(os.path.join(os.path.curdir,"ohlc.csv"))
        self.last_trim = dt.datetime(2021, 1, 1, 0, 0)
        #range/trend flag
        self.flag = None
        self.ml = MLcall()
        self.last_ml_call = None
        self.last_trade = None
        self.last_bid = None
        self.last_ask = None
        self.cur_mean = None
        self.cur_sd = None
        self.cur_zscore = None
        self.trader = None
        # Use ibConnection() for TWS, or create connection for API Gateway
        self.conn = ibConnection() if is_use_gateway else \
            Connection.create(host=host, port=port, clientId=client_id)
        self.__register_data_handlers(self.__on_tick_event,
                                      self.__event_handler)


    def __register_data_handlers(self,
                                 tick_event_handler,
                                 universal_event_handler):
        self.conn.registerAll(universal_event_handler)
        self.conn.unregister(universal_event_handler,
                             ib_message_type.tickSize,
                             ib_message_type.tickPrice,
                             ib_message_type.tickString,
                             ib_message_type.tickGeneric,
                             ib_message_type.tickOptionComputation)
        self.conn.register(tick_event_handler,
                           ib_message_type.tickPrice,
                           ib_message_type.tickSize)

    def __init_stocks_data(self, symbols):
        self.symbols = symbols
#here we'll store tick and size instead of multiple "symbols"
        self.prices = pd.DataFrame(columns=("price","size","ask_price","ask_size","bid_price","bid_size"))  # Init price storage
        if not os.path.exists(self.data_path):
            self.prices.to_csv(self.data_path)
        self.ohlc = pd.DataFrame(columns=("open","high","low","close","volume","count"))  # Init ohlc storage
        if not os.path.exists(self.ohlc_path):
            self.ohlc.to_csv(self.ohlc_path)
        print "checked for csv file"
#Now I have only one "symbol"        
        stock_symbol = self.symbols
        contract = self.ib_util.create_stock_contract(stock_symbol)
        self.stocks_data[stock_symbol] = StockData(contract)
        

    def __request_streaming_data(self, ib_conn):
        # Stream market data. Of note: this enumerate can probably be simplified
        #cannt be bothered for now
        for index, (key, stock_data) in enumerate(
                self.stocks_data.iteritems()):
            ib_conn.reqMktData(index,
                               stock_data.contract,
                               datatype.GENERIC_TICKS_NONE,
                               datatype.SNAPSHOT_NONE)
#            time.sleep(5)

        # Stream account updates DEACTIVATED FOR NOW
#        ib_conn.reqAccountUpdates(True, self.account_code)

    def __request_historical_data(self, ib_conn):
        self.lock.acquire()
        try:
            for index, (key, stock_data) in enumerate(
                    self.stocks_data.iteritems()):
                stock_data.is_storing_data = True
                ib_conn.reqHistoricalData(
                    index,
                    stock_data.contract,
                    time.strftime(datatype.DATE_TIME_FORMAT),
                    datatype.DURATION_1_DAY,
                    datatype.BAR_SIZE_1_MIN,
                    datatype.WHAT_TO_SHOW_TRADES,
                    datatype.RTH_ALL,
                    datatype.DATEFORMAT_STRING)
                time.sleep(1)
#usingthe lock at the end of the cycle
        finally:
            pass


#    def __on_portfolio_update(self, msg):
#        for key, stock_data in self.stocks_data.iteritems():
#            if stock_data.contract.m_symbol == msg.contract.m_symbol:
#                stock_data.update_position(msg.position,
#                                           msg.marketPrice,
#                                           msg.marketValue,
#                                           msg.averageCost,
#                                           msg.unrealizedPNL,
#                                           msg.realizedPNL,
#                                           msg.accountName)
#                return
#
#    def __calculate_pnls(self):
#        upnl, rpnl = 0, 0
#        for key, stock_data in self.stocks_data.iteritems():
#            upnl += stock_data.unrealized_pnl
#            rpnl += stock_data.realized_pnl
#        return upnl, rpnl

    def __event_handler(self, msg):
        if msg.typeName == datatype.MSG_TYPE_HISTORICAL_DATA:
            
            self.__on_historical_data(msg)
        

        elif msg.typeName == datatype.MSG_TYPE_UPDATE_PORTFOLIO:
            pass
#            self.__on_portfolio_update(msg)

        elif msg.typeName == datatype.MSG_TYPE_MANAGED_ACCOUNTS:
            pass

#            self.account_code = msg.accountsList

        elif msg.typeName == datatype.MSG_TYPE_NEXT_ORDER_ID:
            self.order_id = msg.orderId

        else:
            print msg

    def __on_historical_data(self, msg):


        ticker_index = msg.reqId

        if msg.WAP == -1:
            self.__on_historical_data_completed(ticker_index)
        else:
            self.__add_historical_data(ticker_index, msg)

    def __on_historical_data_completed(self, ticker_index):
        self.lock.release()

        self.last_trim = self.ohlc.index[-1]+self.moving_window_period
        print "trim time properly set now %s" % self.last_trim
        self.__run_indicators(self.ohlc)        
        self.ohlc.to_csv(self.ohlc_path)
        #call Azure
        self.flag = self.ml.call_ml(self.ohlc)
        self.last_ml_call = self.last_trim + 5*self.moving_window_period #hackish way to say 5mn
        print self.flag
#        self.ohlc.to_pickle("/Users/maxime_back/Documents/avocado/ohlc.pickle")

    def __add_historical_data(self, ticker_index, msg):
        timestamp = dt.datetime.strptime(msg.date, datatype.DATE_TIME_FORMAT)
        self.__add_ohlc_data(ticker_index, timestamp, msg.open,msg.high,msg.low,msg.close,msg.volume,msg.count)
    
    def __add_ohlc_data(self, ticker_index, timestamp, op, hi ,lo,close,vol,cnt ):
    
            self.ohlc.loc[timestamp, "open"] = float(op)
            self.ohlc.loc[timestamp, "high"] = float(hi)
            self.ohlc.loc[timestamp, "low"] = float(lo)
            self.ohlc.loc[timestamp, "close"] = float(close)
            self.ohlc.loc[timestamp, "volume"] = float(vol)
            self.ohlc.loc[timestamp, "count"] = float(cnt)

    def __on_tick_event(self, msg):
        ticker_id = msg.tickerId
        field_type = msg.field
#        print field_type

        # Store information from last traded price
        if field_type == datatype.FIELD_LAST_PRICE:
            last_price = msg.price
            self.__add_market_data(ticker_id, dt.datetime.now(), last_price, 1)
            self.last_trade = last_price
        if field_type == datatype.FIELD_LAST_SIZE:
            last_size = msg.size
            self.__add_market_data(ticker_id, dt.datetime.now(), last_size, 2)
        if field_type == datatype.FIELD_ASK_PRICE:
            ask_price = msg.price
            self.__add_market_data(ticker_id, dt.datetime.now(), ask_price, 3)
            self.last_ask = ask_price
        if field_type == datatype.FIELD_ASK_SIZE:
            ask_size = msg.size
            self.__add_market_data(ticker_id, dt.datetime.now(), ask_size, 4)
        if field_type == datatype.FIELD_BID_PRICE:
            bid_price = msg.price
            self.__add_market_data(ticker_id, dt.datetime.now(), bid_price, 5)
            self.last_bid = bid_price
        if field_type == datatype.FIELD_BID_SIZE:
            bid_size = msg.size
            self.__add_market_data(ticker_id, dt.datetime.now(), bid_size, 6)
#now to trim the serie every 60 second (logic in trims_serie)     
        if not self.lock.locked():
            self.__trim_data_series()
        #update Zscore spawn
        if 'trader' in locals:
            self.trader.on_tick(self.last_bid,self.last_ask)
                

        # Post-bootstrap - make trading decisions
#        if self.strategy_params.is_bootstrap_completed():
#            self.__recalculate_strategy_parameters_at_interval()
#            self.__perform_trade_logic()
#            self.__update_charts()

    def __add_market_data(self, ticker_index, timestamp, value, col):
        if col == 1:
            self.buffer.append({'time':timestamp, "price": float(value)})
#            
        elif col ==2:
            self.buffer.append({'time':timestamp, "size": float(value)})
        elif col ==3:
            self.buffer.append({'time':timestamp, "ask_price": float(value)})
        elif col ==4:
            self.buffer.append({'time':timestamp, "ask_size": float(value)})
        elif col ==5:
            self.buffer.append({'time':timestamp, "bid_price": float(value)})
        elif col ==6:
            self.buffer.append({'time':timestamp, "bid_size": float(value)})

    def __stream_to_ohlc(self):
        try:
            new_ohlc = pd.DataFrame(columns=("open","high","low","close","volume","count"))
# very likely fuckery to be checked at the cutoff
            t_stmp1 = self.last_trim
            
            t_stmp2 =t_stmp1 + self.moving_window_period
            
            intm2 = self.prices.truncate(after=t_stmp2, before=t_stmp1)
            logging.debug("truncate  ok.Shape: %s", intm2.shape)
            new_ohlc.loc[t_stmp2, "open"] = float(intm2['price'].dropna().head(1))
            
            new_ohlc.loc[t_stmp2, "close"] = float(intm2['price'].dropna().tail(1))
            
            new_ohlc.loc[t_stmp2, "high"] = float(intm2['price'].max())
            
            new_ohlc.loc[t_stmp2, "low"] = float(intm2['price'].min())
            
        
            new_ohlc.loc[t_stmp2, "volume"] = float(intm2['size'].sum())
            
            new_ohlc.loc[t_stmp2, "count"] = float(intm2['size'].count())
            
            return new_ohlc
        except Exception, e:
            print "fuck:", e
            new_ohlc.to_csv(os.path.normpath(os.path.join(os.path.curdir,"new_ohlc.csv")))
            
    def __run_indicators(self, ohlc):
        #hardcoding ML munging parameters now
        ohlc['returns']=ta.ROC(np.asarray(ohlc['close']).astype(float))
        ohlc['sma']=ta.SMA(np.asarray(ohlc['close']).astype(float), 10)
        ohlc['lma']=ta.SMA(np.asarray(ohlc['close']).astype(float), 120)
        ohlc['rsi']=ta.RSI(np.asarray(ohlc['close']).astype(float))
        ohlc['atr']=ta.ATR(np.asarray(ohlc['high']).astype(float),np.asarray(ohlc['low']).astype(float),np.asarray(ohlc['close']).astype(float),10)
        ohlc['monday'] = np.where(ohlc.index.weekday == 0,1,0)
#bellow is because I don't know how to np.where with multiple conditions        
        ohlc['roll'] = np.where(ohlc.index.month % 3 == 0,1,0)
#hackish as balls:
        ohlc.index = ohlc.index.tz_localize("Singapore")
        ohlc["busy"] = np.where(ohlc.index.tz_convert("America/Chicago").hour >= 9,np.where(ohlc.index.tz_convert("America/Chicago").hour <= 14,1,0),0)
                
        
    def __update_norm_params(self):
#        print " updating feeder params for zscore"
        prices = self.prices["price"]
#        print " got prices"
        prices = prices.dropna()
        prices = prices[prices.index > prices.index[-1] - dt.timedelta(seconds=60)]
        last_price = prices.iloc[-1]

        self.cur_mean = np.mean(prices)
        logging.debug("updated mean")
        prices = prices.diff()
        prices = prices.dropna()
        prices = prices**2

        tdiffs = list()
        for i in range(1,len(prices)):
            tdiffs.append((prices.index[i]-prices.index[i-1]).total_seconds())
        prices = prices.ix[1:]
        self.cur_sd = sqrt(sum(prices * tdiffs)/len(prices))
        logging.debug("updated sd")
        self.cur_zscore = (last_price - self.cur_mean)/self.cur_sd



    def __trim_data_series(self):
#        print 'check trim cycle tine considered: %s' % str(self.ohlc.index[-1])        
        if dt.datetime.now() > self.last_trim + self.moving_window_period:
#            print "time condition trim met again"
            intm = pd.DataFrame(self.buffer).set_index('time')
            self.buffer = list()            
            logging.debug("converted list")
            self.prices = self.prices.append(intm)
            logging.debug("appended new prices")
            
 #           print self.ohlc.shape
            self.ohlc = self.ohlc.append(self.__stream_to_ohlc())
            #probably could be optimized            
            self.__run_indicators(self.ohlc)
            with open(self.ohlc_path, 'a') as f:
                    self.ohlc.tail(1).to_csv(f, header=False)
#            print "appended new ohlc. tstp is now:" % str(self.ohlc.index[-1])
            
            self.last_trim = self.last_trim + self.moving_window_period
            print "cleaned buffer"
#           #update parameters for the zscore
            self.__update_norm_params()
            #on minute method
            self.trader.on_minute(self.cur_mean,self.cur_sd,self.flag)

            #store the cutoff (t - 3 moving windows to csv)
            if dt.datetime.now() > self.prices.index[-1] - 3*self.moving_window_period:
                with open(self.data_path, 'a') as f:
                    self.prices[self.prices.index <= self.prices.index[-1] - 3*self.moving_window_period].to_csv(f, header=False)
             #store the cutoff (t - 3 moving windows to csv)
                self.prices = self.prices.truncate(before=self.prices.index[-1] - 3*self.moving_window_period)
                self.prices.to_pickle(os.path.join(os.path.curdir,"prices.pickl"))
        if dt.datetime.now() > self.last_ml_call:
            self.flag = self.ml.call_ml(self.ohlc)
            print self.flag
            self.last_ml_call = self.last_ml_call + 5*self.moving_window_period
#        else:
#        
#            print len(self.buffer)
        
        
    @staticmethod
    def __print_elapsed_time(start_time):
        elapsed_time = time.time() - start_time
        print "Completed in %.3f seconds." % elapsed_time

    def __cancel_market_data_request(self):
        for i, symbol in enumerate(self.symbols):
            self.conn.cancelMktData(i)
            time.sleep(1)

    def start(self, symbols, trade_qty):
        print "HFT model started."
        logging.debug("started requests")

#        self.trade_qty = trade_qty

        self.conn.connect()  # Get IB connection object
        print "connection ok for now"
        self.__init_stocks_data(symbols)
        print "init stock"
        self.__request_streaming_data(self.conn)

 
        start_time = time.time()
        self.__request_historical_data(self.conn)
#maybe I can do without this dd lock (or maybe not, we shall see)
#       self.__wait_for_download_completion()
#        self.strategy_params.set_bootstrap_completed()
#        self.ohlc.to_csv(self.ohlc_path)
        self.__print_elapsed_time(start_time)

        print "Calculating strategy parameters..."
        start_time = time.time()
        time.sleep(250)
        # test existence of parameters
        print "mean"
        print self.cur_mean
        print "sd"
        print self.cur_sd
        print "zscore"
        print self.cur_zscore
        #spawn the middleware
        #__init__(self, init_bid, init_ask, init_zscore, init_mean, init_stdev, init_state, init_flag):
        self.trader = Zscore(self.last_bid,self.last_ask,self.cur_zscore,self.cur_mean,self.cur_sd,"FLAT",self.flag)        

        print "Trading started."
        try:
#            self.__update_charts()
                time.sleep(1)
                print "end of the start cycle"
        
                

        except (Exception, KeyboardInterrupt):
            print "Exception:"
            print "Cancelling...",
            self.__cancel_market_data_request()

            print "Disconnecting..."
            self.conn.disconnect()
            time.sleep(1)
        

            print "Disconnected."
        
        
#        self.store.close()
