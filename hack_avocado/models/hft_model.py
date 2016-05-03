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
#import monitor
from classes.monitor_plotly import Monit_stream
from algos.ZscoreEventDriven import Zscore
import threading
import sys
import os
import logging
import numpy as np
import talib as ta
from math import sqrt
import pytz
from algos.execution_handler import ExecutionHandler

if os.path.exists(os.path.join(os.path.curdir,"log.txt")):
    os.remove(os.path.join(os.path.curdir,"log.txt"))
logging.basicConfig(filename=os.path.normpath(os.path.join(os.path.curdir,"log.txt")),level=logging.DEBUG, format='%(asctime)s %(message)s')


#this will need be checked



class HFTModel:

    def __init__(self, host='localhost', port=4001,
                 client_id=130, is_use_gateway=False,
                 moving_window_period=dt.timedelta(seconds=60)):
        self.tz = pytz.timezone('Singapore')
        self.moving_window_period = moving_window_period
        self.ib_util = IBUtil()


        self.stocks_data = {}  # Dictionary storing StockData objects.REFACTOR
        self.symbols = None  # List of current symbols
        self.account_code = ""
        self.prices = None  # Store last prices in a DataFrame
        self.ohlc = None # I need another store for minute data (I think)
        self.buffer = list()        
        self.trade_qty = 0

        self.lock = threading.Lock()
        self.traffic_light = threading.Event()
        self.thread = None
        self.thread2 = None
        #addition for hdf store
        self.data_path = os.path.normpath(os.path.join(os.path.curdir,"data.csv"))
        self.ohlc_path = os.path.normpath(os.path.join(os.path.curdir,"ohlc.csv"))
        self.last_trim = pytz.timezone("Singapore").localize(dt.datetime(2021, 1, 1, 0, 0))
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


        #self.order_id = self.handler.order_id

        #import monitor
        self.monitor = None
        # Use ibConnection() for TWS, or create connection for API Gateway
        self.conn = ibConnection() if is_use_gateway else \
            Connection.create(host=host, port=port, clientId=client_id)


        self.handler = ExecutionHandler(self.conn)
        #third handler should register properly si Dieu veut
        self.__register_data_handlers(self.__on_tick_event,
                                      self.__event_handler,
                                      self.handler._reply_handler)
        self.order_template = self.handler.create_contract("CL", "FUT", "NYMEX", "201606", "USD")
        self.signal = None
        self.state = None



    def __register_data_handlers(self,
                                 tick_event_handler,
                                 universal_event_handler,order_handler):
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
        self.conn.register(order_handler,
                           ib_message_type.position,
                           ib_message_type.nextValidId,
                           ib_message_type.orderStatus,
                           ib_message_type.openOrder)

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
#Now I have only one "symbol"      TODO: clean that stuff
        stock_symbol = self.symbols
        contract = self.ib_util.create_stock_contract(stock_symbol)
        self.stocks_data[stock_symbol] = StockData(contract)
        

    def __request_streaming_data(self, ib_conn):
        # Stream market data. Of note: this enumerate can probably be simplified
        #cannt be bothered for now TODO: clean this crap, time to be bothered
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
                    datatype.DURATION_2_HR,
                    datatype.BAR_SIZE_1_MIN,
                    datatype.WHAT_TO_SHOW_TRADES,
                    datatype.RTH_ALL,
                    datatype.DATEFORMAT_STRING)
                time.sleep(1)
#usingthe lock at the end of the cycle
        finally:
            pass


    def __on_portfolio_update(self, msg):
        for key, stock_data in self.stocks_data.iteritems():
            if stock_data.contract.m_symbol == msg.contract.m_symbol:
                if dt.datetime.now(self.tz) > self.last_trim + self.moving_window_period:
                    stock_data.update_position(msg.position,
                                               msg.marketPrice,
                                               msg.marketValue)
#                                           ,
#                                           msg.averageCost,
#                                           msg.unrealizedPNL,
#                                           msg.realizedPNL,
#                                           msg.accountName)
                return

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
          
            self.__on_portfolio_update(msg)

        elif msg.typeName == datatype.MSG_TYPE_MANAGED_ACCOUNTS:
            pass

#            self.account_code = msg.accountsList
#this bellow is in a different handler now
        # elif msg.typeName == datatype.MSG_TYPE_OPEN_ORDER:
        #     print("Server Response: %s, %s\n" % (msg.typeName, msg))
        #     print "gottan order open messg"
        #     if msg.orderId == self.handler.order_id and \
        #         not self.handler.fill_dict.has_key(msg.orderId):
        #         self.handler.create_fill_dict_entry(msg)
        #
        # elif msg.typeName == datatype.MSG_TYPE_ORDER_STATUS:
        #     print("Server Response: %s, %s\n" % (msg.typeName, msg))
        #     print "got an order status message"
        #     if msg.filled != 0 and \
        #     self.handler.fill_dict[msg.orderId]["filled"] == False:
        #         self.handler.create_fill(msg)
        #         print "filled at" + msg.lastFillPrice
        #



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
        timestamp = pytz.timezone('Singapore').localize(dt.datetime.strptime(msg.date, datatype.DATE_TIME_FORMAT))
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
            self.__add_market_data(ticker_id, dt.datetime.now(self.tz), last_price, 1)
            self.last_trade = last_price
        if field_type == datatype.FIELD_LAST_SIZE:
            last_size = msg.size
            self.__add_market_data(ticker_id, dt.datetime.now(self.tz), last_size, 2)
        if field_type == datatype.FIELD_ASK_PRICE:
            ask_price = msg.price
            self.__add_market_data(ticker_id, dt.datetime.now(self.tz), ask_price, 3)
            self.last_ask = ask_price
        if field_type == datatype.FIELD_ASK_SIZE:
            ask_size = msg.size
            self.__add_market_data(ticker_id, dt.datetime.now(self.tz), ask_size, 4)
        if field_type == datatype.FIELD_BID_PRICE:
            bid_price = msg.price
            self.__add_market_data(ticker_id, dt.datetime.now(self.tz), bid_price, 5)
            self.last_bid = bid_price
        if field_type == datatype.FIELD_BID_SIZE:
            bid_size = msg.size
            self.__add_market_data(ticker_id, dt.datetime.now(self.tz), bid_size, 6)
#now to trim the serie every 60 second (logic in trims_serie)     
        if not self.lock.locked():
            # print"lock locked call trim data"
            self.__trim_data_series()
        #update Zscore spawn
        if self.cur_zscore is not None:
            # print "update zscore traffic light"
            self.traffic_light.set()
            
        if self.trader is not None:
            self.trader.on_tick(self.last_bid,self.last_ask)
            self.signal = self.trader.update_signal()
            # print self.signal
            self.state = self.trader.update_state()
            # print self.state
        if self.handler is not None:
            self.execute_trade(self.signal, self.last_bid, self.last_ask)
            #Hackish af
            if self.handler is not None and self.monitor is not None:

                self.monitor.update_fills(self.handler.passpass())
                


    def __add_market_data(self, ticker_index, timestamp, value, col):
        if col == 1:
            self.buffer.append({'time':timestamp, "price": float(value)})
#           #momitoring
            if self.monitor is not None:
                self.monitor.update_data_point(float(value),self.cur_mean,self.cur_sd, self.flag)
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
            print t_stmp1
            
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
        # ohlc.index = ohlc.index.tz_localize("Singapore")
        ohlc["busy"] = np.where(ohlc.index.tz_convert("America/Chicago").hour >= 9,np.where(ohlc.index.tz_convert("America/Chicago").hour <= 14,1,0),0)
                
        
    def __update_norm_params(self):
#        print " updating feeder params for zscore"
        prices = self.prices["price"]
#        print " got prices"
        prices = prices.dropna()
        prices = prices[prices.index > prices.index[-1] - dt.timedelta(seconds=60)]
        if len(prices) !=0:
            last_price = prices.iloc[-1]

            self.cur_mean = np.mean(prices)
            #logging.debug("updated mean")
            print last_price
            prices = prices.diff()
            prices = prices.dropna()
            prices = prices**2

            tdiffs = list()
            for i in range(1,len(prices)):
                tdiffs.append((prices.index[i]-prices.index[i-1]).total_seconds())
            prices = prices.ix[1:]
            self.cur_sd = sqrt(sum(prices * tdiffs)/len(prices))
            #logging.debug("updated sd")
            self.cur_zscore = (last_price - self.cur_mean)/self.cur_sd
            print(self.cur_zscore)


    def __trim_data_series(self):
#        print 'check trim cycle time considered: %s' % str(self.ohlc.index[-1])
#        print "datetime now %s" % str(dt.datetime.now(self.tz))
#        print "last trim + moving window %s" % str(self.last_trim + self.moving_window_period)
        if dt.datetime.now(self.tz) > self.last_trim + self.moving_window_period:
            print "time condition trim met again"
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
            if self.trader is not None:
                print "call on minute"
                self.trader.on_minute(self.cur_mean,self.cur_sd,self.flag)

            #store the cutoff (t - 3 moving windows to csv)
            if dt.datetime.now(self.tz) > self.prices.index[-1] - 3*self.moving_window_period:
                with open(self.data_path, 'a') as f:
                    self.prices[self.prices.index <= self.prices.index[-1] - 3*self.moving_window_period].to_csv(f, header=False)
             #store the cutoff (t - 3 moving windows to csv)
                self.prices = self.prices.truncate(before=self.prices.index[-1] - 3*self.moving_window_period)
                self.prices.to_pickle(os.path.join(os.path.curdir,"prices.pickl"))
        if dt.datetime.now(self.tz) > self.last_ml_call:
            self.flag = self.ml.call_ml(self.ohlc)
            print self.flag
            self.last_ml_call = self.last_ml_call + 5*self.moving_window_period
#        else:
#        
#            print len(self.buffer)

     #############
     # This is transposed from Zscore in part
     ##############
     #

    def execute_trade(self,signal,last_bid,last_ask):
        if signal == "BUY" or signal =="SELL":
            print "got a signal, passing to handler"
            if not self.handler.is_trading:
                self.handler.place_trade(signal,self.last_bid,self.last_trade)



    @staticmethod
    def __print_elapsed_time(start_time):
        elapsed_time = time.time() - start_time
        print "Completed in %.3f seconds." % elapsed_time

    def __cancel_market_data_request(self):
        for i, symbol in enumerate(self.symbols):
            self.conn.cancelMktData(i)
            time.sleep(1)
    
    def spawn(self):
        print "zscore thread spawned"        
        self.traffic_light.wait()
        print "light's green"
        self.trader = Zscore(self.last_bid,self.last_ask,self.cur_zscore,self.cur_mean,self.cur_sd,"FLAT",self.flag,self.conn)
        #print "killing them all"
        #self.handler.kill_em_all()

    def spawn_monitor(self):
        print "monitor thread spawned"        
        self.traffic_light.wait()
        
        self.monitor = Monit_stream()

    def start(self, symbols, trade_qty):
        print "HFT model started."
        logging.debug("started requests")


#        self.trade_qty = trade_qty

        self.conn.connect()  # Get IB connection object
#wasting  my time here
#        print "time at IB"        
#        print self.conn.reqCurrentTime()
        
        self.__init_stocks_data(symbols)
        print "init stock"
        self.__request_streaming_data(self.conn)

 
        start_time = time.time()
        self.__request_historical_data(self.conn)

        self.__print_elapsed_time(start_time)

        print "Calculating strategy parameters..."
        start_time = time.time()
        
        
        print "zscore check coming"        
        
        self.thread = threading.Thread(target=self.spawn())
        self.thread.start()
        self.thread2 = threading.Thread(target=self.spawn_monitor)
        self.thread2.start()

        def main_loop():
            while 1:
                # do your stuff...
                time.sleep(0.1)

        try:
            main_loop()
        

        
                

        except (Exception, KeyboardInterrupt):
            print "Exception:"
            print "Cancelling...",
            self.__cancel_market_data_request()
            print "killing all orders"
            self.handler.kill_em_all()
            self.handler.cancel_pos()
            self.handler.fill_dict[sorted(self.handler.fill_dict)[-1]]["filled"] = True
            self.handler.save_pickle()
            # self.monitor.close_stream()
            print "Disconnecting..."
            time.sleep(10)
            self.conn.disconnect()
            time.sleep(1)
        

            print "Disconnected."

    def stop(self):
        os.remove(os.path.normpath(os.path.join(os.path.curdir,"data.csv")))
        os.remove(os.path.normpath(os.path.join(os.path.curdir,"ohlc.csv")))
        self.__cancel_market_data_request()
        #self.monitor.close_stream()
        print "Disconnecting..."
        self.conn.disconnect()
        
#        self.store.close()

    def spawn_test(self):
        self.traffic_light.wait()
        print "fuck spawns"
