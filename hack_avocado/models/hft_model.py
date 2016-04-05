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
import params.ib_data_types as datatype
#from params.strategy_parameters import StrategyParameters
#from classes.chart import Chart
import threading
import sys
import os

#this will need be checked



class HFTModel:

    def __init__(self, host='localhost', port=4001,
                 client_id=130, is_use_gateway=False, evaluation_time_secs=20,
                 resample_interval_secs='30s',
                 moving_window_period=dt.timedelta(seconds=60)):
        self.moving_window_period = moving_window_period
#        self.chart = Chart()
        self.ib_util = IBUtil()

        # Store parameters for this model
#        self.strategy_params = StrategyParameters(evaluation_time_secs,
#                                                  resample_interval_secs)

        self.stocks_data = {}  # Dictionary storing StockData objects.
        self.symbols = None  # List of current symbols
        self.account_code = ""
        self.prices = None  # Store last prices in a DataFrame
        self.ohlc = None # I need another store for minute data (I think)        
        self.trade_qty = 0
        self.order_id = 0
        self.lock = threading.Lock()
        #addition for hdf store
        self.data_path = os.path.normpath("/Users/maxime_back/Documents/avocado/data.csv")
        self.ohlc_path = os.path.normpath("/Users/maxime_back/Documents/avocado/ohlc.csv")
        self.last_trim = dt.datetime.now()


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
        print "contracts initated"

    def __request_streaming_data(self, ib_conn):
        # Stream market data. Of note: this enumerate can probably be simplified
        #cannt be bothered for now
        for index, (key, stock_data) in enumerate(
                self.stocks_data.iteritems()):
            ib_conn.reqMktData(index,
                               stock_data.contract,
                               datatype.GENERIC_TICKS_NONE,
                               datatype.SNAPSHOT_NONE)
            time.sleep(5)

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
        finally:
            self.lock.release()
            self.last_trim = dt.datetime.now()

#    def __wait_for_download_completion(self):
#        is_waiting = True
#        while is_waiting:
#            is_waiting = False
#
#            self.lock.acquire()
#            try:
#                for symbol in self.stocks_data.keys():
#                    if self.stocks_data[symbol].is_storing_data:
#                        is_waiting = True
#            finally:
#                self.lock.release()
#
#            if is_waiting:
#                time.sleep(1)


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
        print msg

        ticker_index = msg.reqId

        if msg.WAP == -1:
            self.__on_historical_data_completed(ticker_index)
        else:
            self.__add_historical_data(ticker_index, msg)

    def __on_historical_data_completed(self, ticker_index):
        self.lock.acquire()
        try:
            symbol = self.symbols#[ticker_index]
            self.stocks_data[symbol].is_storing_data = False
        finally:
            self.lock.release()

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
        if field_type == datatype.FIELD_LAST_SIZE:
            last_size = msg.size
            self.__add_market_data(ticker_id, dt.datetime.now(), last_size, 2)
        if field_type == datatype.FIELD_ASK_PRICE:
            ask_price = msg.price
            self.__add_market_data(ticker_id, dt.datetime.now(), ask_price, 3)
        if field_type == datatype.FIELD_ASK_SIZE:
            ask_size = msg.size
            self.__add_market_data(ticker_id, dt.datetime.now(), ask_size, 4)
        if field_type == datatype.FIELD_BID_PRICE:
            bid_price = msg.price
            self.__add_market_data(ticker_id, dt.datetime.now(), bid_price, 5)
        if field_type == datatype.FIELD_BID_SIZE:
            bid_size = msg.size
            self.__add_market_data(ticker_id, dt.datetime.now(), bid_size, 6)
#now to trim the serie every 60 second        
        if dt.datetime.now() > self.last_trim + self.moving_window_period:
            self.__trim_data_series()

        # Post-bootstrap - make trading decisions
#        if self.strategy_params.is_bootstrap_completed():
#            self.__recalculate_strategy_parameters_at_interval()
#            self.__perform_trade_logic()
#            self.__update_charts()

    def __add_market_data(self, ticker_index, timestamp, value, col):
        if col == 1:
            self.prices.loc[timestamp, "price"] = float(value)
#            self.prices = self.prices.fillna(method='ffill')  # Clear NaN values ## Not sure how to handle this one
            self.prices.sort_index(inplace=True)
        elif col ==2:
            self.prices.loc[timestamp, "size"] = float(value)
#            self.prices = self.prices.fillna(method='ffill')  # Clear NaN values
            self.prices.sort_index(inplace=True)
        elif col ==3:
            self.prices.loc[timestamp, "ask_price"] = float(value)
#            self.prices = self.prices.fillna(method='ffill')  # Clear NaN values
            self.prices.sort_index(inplace=True)
        elif col ==4:
            self.prices.loc[timestamp, "ask_size"] = float(value)
#            self.prices = self.prices.fillna(method='ffill')  # Clear NaN values
            self.prices.sort_index(inplace=True)
        elif col ==5:
            self.prices.loc[timestamp, "bid_price"] = float(value)
#            self.prices = self.prices.fillna(method='ffill')  # Clear NaN values
            self.prices.sort_index(inplace=True)
        elif col ==6:
            self.prices.loc[timestamp, "bid_size"] = float(value)
#            self.prices = self.prices.fillna(method='ffill')  # Clear NaN values
            self.prices.sort_index(inplace=True)

    def __stream_to_ohlc(self):
#        stream = stream[["price","size"]]
#        stream.is_copy = False
#        stream.dropna(inplace=True, how='all')
        
        try:
            new_ohlc = pd.DataFrame(columns=("open","high","low","close","volume","count"))
# very likely fuckery to be checked at the cutoff
            t_stmp = self.prices.first_valid_index().replace(second=0, microsecond=0)        
            new_ohlc.loc[t_stmp, "open"] = float(self.prices['price'].dropna().head(1))
            new_ohlc.loc[t_stmp, "close"] = float(self.prices['price'].dropna().tail(1))
            new_ohlc.loc[t_stmp, "high"] = float(self.prices['price'].max())
            new_ohlc.loc[t_stmp, "low"] = float(self.prices['price'].min())
            new_ohlc.loc[t_stmp, "low"] = float(self.prices['price'].min())
            new_ohlc.loc[t_stmp, "volume"] = float(self.prices['size'].sum())
            new_ohlc.loc[t_stmp, "count"] = float(self.prices['size'].count())
            return new_ohlc
        except Exception, e:
            print "fuck:", e
            print self.prices
        


    def __trim_data_series(self):
        print "trim started"
        
        cutoff_timestamp = dt.datetime.now()
        #clean up for overlap of tick vs 1mn ohlc bar
        self.prices = self.prices[self.prices.index >= self.last_trim]
        
        with open(self.data_path, 'a') as f:
            self.prices[self.prices.index <= cutoff_timestamp].to_csv(f, header=False)
        #append new ohlc row ++ cutoff probably ficked
        
        self.ohlc = self.ohlc.append(self.__stream_to_ohlc())
        

        self.prices = self.prices[self.prices.index >= cutoff_timestamp]
#        self.strategy_params.trim_indicators_series(cutoff_timestamp)

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
        

#        self.trade_qty = trade_qty

        self.conn.connect()  # Get IB connection object
        print "connection ok for now"
        self.__init_stocks_data(symbols)
        print "init stock"
        self.__request_streaming_data(self.conn)

        print self.conn
        start_time = time.time()
        self.__request_historical_data(self.conn)
#maybe I can do without this dd lock (or maybe not, we shall see)
#       self.__wait_for_download_completion()
#        self.strategy_params.set_bootstrap_completed()
        self.ohlc.to_csv(self.ohlc_path)
        self.__print_elapsed_time(start_time)

        print "Calculating strategy parameters..."
        start_time = time.time()
#        self.__calculate_strategy_params()
#        self.__print_elapsed_time(start_time)

        print "Trading started."
        try:
#            self.__update_charts()
                time.sleep(1)
                print "end of the start cycle"
        
                

        except Exception, e:
            print "Exception:", e
            print "Cancelling...",
            self.__cancel_market_data_request()

            print "Disconnecting..."
            self.conn.disconnect()
            time.sleep(1)
            self.prices.to_hdf(self.store)
            self.store.close()
        

            print "Disconnected."
        
        
#        self.store.close()
