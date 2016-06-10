"""
A model leveraging a Microsoft Azure instance and Interactive Brokers' API to trade CME's WTI
Do not try this at home

"""
import pandas as pd
from ib.opt import ibConnection, message as ib_message_type
from ib.opt import Connection

import datetime as dt
import time
from ib.ext.Contract import Contract
from multiprocessing import Event, Lock
import concurrent.futures
import sys
import os
import logging
import numpy as np
import talib as ta
from math import sqrt
import pytz

from classes.ib_util import IBUtil
from classes.ml_api_call import MLcall
from algos.execution_handler2 import ExecutionHandler
from params import settings
import params.ib_data_types as datatype


if os.path.exists(os.path.join(os.path.curdir,"log.txt")):
    os.remove(os.path.join(os.path.curdir,"log.txt"))

print "Clearing logger"
logging.getLogger('').handlers = []



class HFTModel:

    def __init__(self, host='localhost', port=4001,
                 client_id=130, is_use_gateway=False,
                 moving_window_period=dt.timedelta(seconds=60), test=False):
        logging.basicConfig(format='%(asctime)s %(message)s')
        self.test_logger = logging.getLogger('hftModelLogger')
        self.test_logger.setLevel(logging.INFO)
        self.test = test
        self.tz = pytz.timezone('Singapore')
        self.moving_window_period = moving_window_period
        self.ib_util = IBUtil()
        self.symbols = None  # List of current symbols
        self.account_code = ""
        self.prices = None  # Store last prices in a DataFrame
        self.ohlc = None # I need another store for minute data (I think)
        self.buffer = list()
        self.trade_qty = 0
        self.traffic_light = Event()
        self.ohlc_ok = Lock()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=6)
        self.timekeeper = None
        self.parser = None
        self.execution = None
        self.strategy = None
        self.handler = None
        self.data_path = os.path.normpath(os.path.join(os.path.curdir,"data.csv"))
        self.ohlc_path = os.path.normpath(os.path.join(os.path.curdir,"ohlc.csv"))
        self.flag = None
        self.event_market_on = Event()
        self.ml = MLcall()
        self.last_trim = None
        self.last_ml_call = None
        self.cur_mean = None
        self.cur_sd = None


        # Use ibConnection() for TWS, or create connection for API Gateway
        self.conn = Connection.create(host=host, port=port, clientId=client_id)

        if not self.test:
            self.handler = ExecutionHandler(self.conn)



        #third handler should register properly si Dieu veut
        if self.test:
            self.__register_data_handlers(self.null_handler,
                                          self.__event_handler,
                                          self.null_handler)
        else:
            self.__register_data_handlers(self.handler.on_tick_event,
                                          self.__event_handler,
                                          self.handler._reply_handler)
        if self.test:
            self.order_template = self.create_contract(settings.SYMBOL,
                                                       settings.SECURITY,
                                                       settings.EXCHANGE,
                                                       settings.EXPIRY,
                                                       settings.CURRENCY)
        else:
            self.order_template = self.handler.create_contract(settings.SYMBOL,
                                                       settings.SECURITY,
                                                       settings.EXCHANGE,
                                                       settings.EXPIRY,
                                                       settings.CURRENCY)
        self.signal = None
        self.state = None



    def _market_is_open(self):

        tz_cme = pytz.timezone('America/Chicago')
        cme_now = self.now.astimezone(tz_cme)

        if cme_now.weekday() in [0, 1, 2, 3] and (
                cme_now >= cme_now.replace(hour=17, minute=0, second=0) or cme_now < cme_now.replace(hour=16,
                                                                                                     minute=0,
                                                                                                     second=0)):
            self.event_market_on.set()
        elif cme_now.weekday() is 4 and cme_now < cme_now.replace(hour=16, minute=0, second=0):
            self.event_market_on.set()
        elif cme_now.weekday() is 6 and cme_now >= cme_now.replace(hour=17, minute=0, second=0):
            self.event_market_on.set()
        else:
            self.event_market_on.clear()
            # print "market is on:"
            # print self.event_market_on.is_set()



    def time_keeper(self):

        while True:
            print "executing and trading flag set?"
            print self.handler.executing.is_set()
            print self.handler.trading.is_set()


            self.now = pytz.timezone('Singapore').localize(dt.datetime.now())
            self._market_is_open()
            # OHLC call
            if self.last_trim is None:
                self.last_trim = self.now

            if self.now > self.last_trim + dt.timedelta(minutes=1):
                if self.parser.running():
                    self.test_logger.error("parser thread is alive - timekeeper")
                if self.strategy.running():
                    self.test_logger.error("strategy thread is alive - timekeeper")
                if self.execution.running():
                    self.test_logger.error("execution thread is alive - timekeeper")
                else:
                    self.test_logger.error("!!!execution thread  is DEAD- timekeeper")

                self.test_logger.error("timekeeper minute call")
                try:
                    self.__request_historical_data(self.conn,initial=False)
                except:
                    self.test_logger.error("req of update historicals failed - timekeep/hft")
                try:
                    self.__run_indicators(self.ohlc)
                except:
                    self.test_logger.error("runnning indicators failed - timekeep/hft")


                self.conn.reqPositions()
                self.update_norm_params()


                if self.test:
                    print self.ohlc.tail()


                time.sleep(5)#TODO horrible, horrible, but can't be bothered with a lock right now

            # ML Call
            if self.last_ml_call is None:
                self.last_ml_call = self.now

            if self.now > self.last_ml_call + dt.timedelta(minutes=5):

                #logging.DEBUG("ML Call")
                #logging.DEBUG(str(self.ohlc))
                try:
                    self.flag = self.ml.call_ml(self.ohlc)

                    self.handler.flag = self.flag  # this is stupid todo why why why
                    self.test_logger.error("I believe we will " + self.handler.flag)
                    self.last_ml_call = self.now
                except:
                    self.test_logger.error("!!! Call ML failed - timekeeper/hft")



            time.sleep(1)

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
                       ib_message_type.openOrder,
                       ib_message_type.error)

    def __init_stocks_data(self, symbols):#todo brutal hack-through
        self.symbols = symbols
#here we'll store tick and size instead of multiple "symbols"
        self.prices = pd.DataFrame(columns=("price","size","ask_price","ask_size","bid_price","bid_size"))#todomoved to execution  # Init price storage
        if not os.path.exists(self.data_path):
            self.prices.to_csv(self.data_path)
        self.ohlc = pd.DataFrame(columns=("open","high","low","close","volume","count"))  # Init ohlc storage
        if not os.path.exists(self.ohlc_path):
            self.ohlc.to_csv(self.ohlc_path)
        print "checked for csv file"
#Now I have only one "symbol"      TODO: clean that stuff
        stock_symbol = self.symbols
        contract = self.ib_util.create_stock_contract(stock_symbol)
        #self.stocks_data[stock_symbol] = StockData(contract)

    #this is redundant but required to scaffold/test
    def create_contract(self, symbol, sec_type, exch, expiry, curr):
        """Create a Contract object defining what will
        be purchased, at which exchange and in which currency.

        symbol - The ticker symbol for the contract
        sec_type - The security type for the contract ('FUT' = Future)
        exch - The exchange to carry out the contract on
        prim_exch - The primary exchange to carry out the contract on
        curr - The currency in which to purchase the contract"""
        contract = Contract()
        contract.m_symbol = symbol
        contract.m_secType = sec_type
        contract.m_exchange = exch
        contract.m_expiry = expiry
        contract.m_currency = curr
        return contract

    def __request_streaming_data(self, ib_conn):
        # Stream market data.
            ib_conn.reqMktData(1,
                               self.order_template,
                               datatype.GENERIC_TICKS_NONE,
                               datatype.SNAPSHOT_NONE)
#            time.sleep(5)


    def __request_historical_data(self, ib_conn, initial=True):
        """ the same method can be used for scheduled calls"""
        # self.ohlc_ok.acquire()
        if initial:
            duration = datatype.DURATION_2_HR
        else:
            duration = datatype.DURATION_1_MIN
        ib_conn.reqHistoricalData(
            1,
            self.order_template,
            time.strftime(datatype.DATE_TIME_FORMAT),
            duration,
            datatype.BAR_SIZE_1_MIN,
            datatype.WHAT_TO_SHOW_TRADES,
            datatype.RTH_ALL,
            datatype.DATEFORMAT_STRING)
        time.sleep(1)




    def __event_handler(self, msg):
        if msg.typeName == datatype.MSG_TYPE_HISTORICAL_DATA:

            self.__on_historical_data(msg)


        elif msg.typeName == datatype.MSG_TYPE_UPDATE_PORTFOLIO:
            pass
            #self.__on_portfolio_update(msg)

        elif msg.typeName == datatype.MSG_TYPE_MANAGED_ACCOUNTS:
            pass

        else:
            print msg

    def null_handler(self,msg):
        pass




    def __on_historical_data(self, msg):


        ticker_index = msg.reqId

        if msg.WAP == -1:
            self.__on_historical_data_completed()
        else:

            self.__add_historical_data(ticker_index, msg)

    def __on_historical_data_completed(self):
        #self.lock.release()

        self.last_trim = self.ohlc.index[-1]+self.moving_window_period
        print "trim time properly set now %s" % self.last_trim
        print "start position is:" + str(self.handler.position)
        self.__run_indicators(self.ohlc)
        self.ohlc.to_csv(self.ohlc_path)



    def __add_historical_data(self, ticker_index, msg):
        if self.test:
            print "adding  histo line"
        timestamp = pytz.timezone('Singapore').localize(dt.datetime.strptime(msg.date, datatype.DATE_TIME_FORMAT))
        self.__add_ohlc_data(timestamp, msg.open,msg.high,msg.low,msg.close,msg.volume,msg.count)

    def __add_ohlc_data(self, timestamp, op, hi ,lo,close,vol,cnt):

            self.ohlc.loc[timestamp, "open"] = float(op)
            self.ohlc.loc[timestamp, "high"] = float(hi)
            self.ohlc.loc[timestamp, "low"] = float(lo)
            self.ohlc.loc[timestamp, "close"] = float(close)
            self.ohlc.loc[timestamp, "volume"] = float(vol)
            self.ohlc.loc[timestamp, "count"] = float(cnt)

#




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


    def update_norm_params(self):

        try:
            prices = self.handler.prices["price"]
        except:
            self.test_logger.error("getting price from handler failed - update norm/hft")

        sgp_tz = pytz.timezone('Singapore')

        try:
            prices.index = prices.index.tz_localize(sgp_tz)
            #prices.to_pick  (os.path.join(os.path.curdir,"prices_f_norm.csv"))
        except:
            self.test_logger.error("prices localization failed - update norm/hft")
    #
        try:
            prices = prices.dropna()
        except:
            self.test_logger.error("dropna failed - update norm/hft")



        try:
            if prices.index.max()-prices.index.min() > dt.timedelta(seconds=60) and self.last_trim is not None:
                prices = prices[prices.index > prices.index.max()-dt.timedelta(seconds=60)]#todo no trim of prices as of now
                print "trimmed prices"
                print prices
                #prices.to_pickle(os.path.join(os.path.curdir(), "prices.pkl"))
        except:

            self.test_logger.error("minute trim of prices failed - update norm/hft")

        if len(prices) !=0:
            last_price = prices.iloc[-1]
            try:
                my_cur_mean = round(np.mean(prices),2)
                self.handler.cur_mean = my_cur_mean
            except:
                self.test_logger.error("mean update failed- update norm/hft")

            try:
                prices = prices.diff()
                prices = prices.dropna()
                prices = prices**2
            except:
                self.test_logger.error("diff/square failed- update norm/hft")

            tdiffs = list()
            try:
                for i in range(1,len(prices)):
                    tdiffs.append((prices.index[i]-prices.index[i-1]).total_seconds())
                prices = prices.ix[1:]
                print prices
            except:

                self.test_logger.error("time diffs creation failed - update norm/hft")
            try:

                my_cur_sd = round(sqrt(sum(prices * tdiffs)/len(prices)), 2)
                self.handler.cur_sd = my_cur_sd

            except:
                self.test_logger.error("StDev udpate failed - update norm/hft")
                self.handler.cur_sd = 2 * settings.STOP_OFFSET  # in case the stdev failed

        self.test_logger.error("update norm parameters completed - update norm/hft")
        self.last_trim = self.now

    def __cancel_market_data_request(self):

        self.conn.cancelMktData(1)
        time.sleep(1)

    def spawn(self):
        print "execution thread spawned"

        self.handler = ExecutionHandler(self.conn)



    def start(self, symbols):
        self.test_logger.info("Started Requests !!!!")
        self.conn.connect()  # Get IB connection object
        self.__init_stocks_data(symbols)
        self.test_logger.info("Init Stock")
        self.test_logger.info("Request Market Data")
        self.__request_streaming_data(self.conn)
        self.test_logger.info("Request Position")
        self.conn.reqPositions()
        self.test_logger.info("Request Historicals")
        self.__request_historical_data(self.conn)
        time.sleep(1)
        if self.handler.position !=0:
            self.test_logger.info("Squaring off for a clean start")
            self.handler.neutralize()
        try:
            #self.time_keeper()
            time.sleep(5)
            self.test_logger.info("I hope Ihave ohlc now, from hft")

            print self.ohlc.tail(5)

            self.test_logger.info("call ML first time - HFT")

            self.flag = self.ml.call_ml(self.ohlc)

            self.handler.flag = self.flag  # this is stupid
            time.sleep(3)
            print "I believe we will "+ self.flag

            self.test_logger.info("Spawn concurrent processes")

            self.timekeeper = self.executor.submit(self.time_keeper)
            self.test_logger.info("Time keeper spawned")
            time.sleep(1)
            self.parser = self.executor.submit(self.handler.queue_parser)
            self.test_logger.info("Parser spawned")
            time.sleep(5)

            self.update_norm_params()
            self.test_logger.info("First normalized parameters passed")
            time.sleep(5)
            self.test_logger.info("Spawning strategy handler")

            self.strategy = self.executor.submit(self.handler.trading_loop)
            self.test_logger.info("Spawning execution handler")
            self.execution = self.executor.submit(self.handler.order_execution)






        except (Exception, KeyboardInterrupt):
            print "Exception:"
            print "Cancelling...",
            self.__cancel_market_data_request()
            print "killing all orders"
            self.handler.kill_em_all()


            # self.monitor.close_stream()
            print "Disconnecting..."
            time.sleep(5)
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

    def rekindle_execution(self):
        self.execution = self.executor.submit(self.handler.trading_loop)



if __name__ == "__main___":
    print "I'm testing stuff"
    model = HFTModel(host='localhost',
                     port=4001,
                     client_id=101,
                     is_use_gateway=False, test=True)
    model.start("CL")

    time.sleep(15)

    model.conn.disconnect()
