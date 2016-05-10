
import datetime as dt
import time
import pickle
import os
from multiprocessing import Queue, Process, log_to_stderr, Value
import concurrent.futures
import sys
import ib.ext
from ib.ext.Contract import Contract
from ib.ext.Order import Order

from ib.opt import ibConnection, Connection, message as ib_message_type
# from ib.ext.EWrapper import EWrapper
import logging
import pandas as pd
from params import settings


# (*) To communicate with Plotly's server, sign in with credentials file
import plotly.plotly as py

# (*) Useful Python/Plotly tools
import plotly.tools as tls

# (*) Graph objects to piece together plots
from plotly.graph_objs import *
import datetime as dt
from params import settings
import params.ib_data_types as datatype

logging.basicConfig(filename=os.path.normpath(os.path.join(os.path.curdir, "log.txt")),
                    level=logging.DEBUG,
                    format='%(asctime)s %(message)s')

class ExecutionHandler(object):
    """
    Handles order execution via the Interactive Brokers API
    """

    def __init__(self, ib_conn,test=False):
        # initialize
        self.test = test
        self.ib_conn = ib_conn
        self.valid_id = None
        self.position = None
        self.contract = self.create_contract("CL",'FUT', 'NYMEX', '201606','USD')
        self.is_trading = False
        #will need  a test for pickle existence TODO panda the pickle or something
        self.zscore = None
        self.zscore_thresh = settings.Z_THRESH
        self.thresh_tgt = 0
        self.flag = None
        self.hist_flag = None
        self.main_order = {"id": None,
                           "order": None,
                           "timeout": None,
                           "filled": False,
                           "active": False}
        self.stop_order = {"id": None,
                           "order": None,
                           "filled": None,
                           "active": False}
        self.profit_order ={"id": None,
                           "order": None,
                           "filled": False,
                           "active": False}
        #trade data management. in a Value class #TODO I dont have Noneinitial type, but 0, so..
        self.mkt_data_queue = Queue()
        #trading values
        self.last_trade = None#todo this is not thread safe
        self.last_bid = None
        self.last_ask = None
        self.last_fill = None
        self.cur_mean = None
        self.cur_sd = None
        self.flag = None
        #strapping the monitor there
        if not self.test:
            self.monitor = Monit_stream()
        #working parameters
        self.watermark = 0
        self.stop_offset = settings.STOP_OFFSET
        self.stop = 0
        self.shelflife = 5
        #fill dict as a list
        self.fill_dict = []
        #store fills in CSV for post-mortem
        self.csv = self.data_path = os.path.normpath(os.path.join(os.path.curdir,"fills.csv"))
        #moved from HFT
        self.prices = pd.DataFrame(columns=["price"])






    def _reply_handler(self, msg):
        #valid id handler
        if msg.typeName == "nextValidId" and self.valid_id is None:
            self.valid_id =int(msg.orderId)


        #position handler
        if msg.typeName == "position":
            self.position = int(msg.pos)

        # Handle open order orderId processing
        if msg.typeName == "openOrder":
            #print "ack " + str(msg.orderId)
            print msg
            #zboub = msg.order
            #print zboub.m_action
         #   self.create_fill_dict_entry(msg.orderId)
        # # Handle Fills
        if msg.typeName == "orderStatus":
            # print msg
            if msg.filled != 0:
                self.create_fill(msg)

        if msg.typeName == "error":
            #pass
            print "error intercepted"
            print msg





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

    def create_order(self, order_type, quantity, action, lmt_price=""):
        """Create an Order object (Market/Limit) to go long/short.

        order_type - 'MKT', 'LMT' for Market or Limit orders
        quantity - Integral number of assets to order
        action - 'BUY' or 'SELL'"""
        order = Order()
        order.m_orderType = order_type
        order.m_totalQuantity = quantity
        order.m_action = action
        order.m_lmtPrice = lmt_price
        return order

    def queue_parser(self):
        """
        the parser will update data points in memory (possibly with manager.dict()) and plotly
        :return:
        """
        print "parser is started"
        while True:
            # print "parsing is not effed"
            # print "queue is empty:"
            # print self.mkt_data_queue.empty()
            # print self.mkt_data_queue.get()
            msg = self.mkt_data_queue.get()
            if msg["type"] == "last_trade":
                # print "that was a trade"
                self.last_trade = msg["value"]
                self.prices.loc[msg['time'],"price"] = msg["value"]


                #self.prices.append()
                # print "test pandas"
                # print str(self.cur_mean)
                # print str(self.cur_sd)
                # print str(self.flag)

                if self.cur_mean is not None  and self.cur_sd is not None and self.flag is not None:
                    self.monitor.update_data_point(msg,self.cur_mean,self.cur_sd,self.flag)
            if msg["type"] == "ask_price":
                self.last_ask = msg["value"]

            if msg["type"] == "bid_price":
                self.last_bid = msg["value"]

    def trading_loop(self):
        """
        So, this one will not take any outside input, but loop forever instead. *Should be thread-safe*

        """
        print "in trader loop"
        x_delay = 0.3
        while True:
            try:

                if self.cur_mean is not None and self.cur_sd is not None and self.last_trade is not None:
                    #logging.debug("check ontick loop")
                    zscore = (self.last_trade - self.cur_mean)/self.cur_sd
                    # self.last_bid = cur_bid
                    # self.last_ask = cur_ask
                    # self.last_trade = cur_trade
                    # self.flag = cur_flag
                    # self.cur_mean = cur_mean
                    # self.cur_sd =cur_sd
                    # if not (self.last_trade or self.cur_mean or self.cur_sd or self.flag) == 0:
                    #     self.monitor.update_data_point(self.last_trade, self.cur_mean, self.cur_sd, self.flag)
                    if self.hist_flag is None:
                        self.hist_flag = self.flag
                        print "updated hist flag"
                    #check change of state and kill positions TODO this is simplistic
                    if self.hist_flag != self.flag and self.main_order["active"]:
                        print "change of state, killing main position"
                        logging.debug("exec - change of state, killing position")
                        self.execute_order(self.stop_order["order"])
                        self.hist_flag = self.flag
                        return

                    #first, checkzscore and do an order
                    #print "current z " + str(self.zscore) + " vs " + str(self.zscore_thresh)
                    if abs(zscore) >= self.zscore_thresh and not self.main_order["active"]: #need to check for other status
                        logging.debug("exec - zscore condition")
                        if zscore >= self.zscore_thresh:
                            if self.flag == "trend":
                                action = "BUY"

                            if self.flag == "range":
                                action = "SELL"

                        if zscore <= -self.zscore_thresh:
                            if self.flag == "trend":
                                action = "SELL"

                            if self.flag == "range":
                                action = "BUY"

                        if action == "BUY":
                            naction = "SELL"
                            price = self.last_bid
                            offset = -self.stop_offset
                        if action == "SELL":
                            naction = "BUY"
                            price = self.last_ask
                            offset = self.stop_offset

                        #spawn main order, stop and profit
                        self.main_order["id"] = self.valid_id
                        self.main_order["order"] = self.create_order("LMT",1,action,price)
                        self.stop_order["id"] = self.valid_id+1
                        self.stop_order["order"] = self.create_order("MKT", 1, naction)
                        self.profit_order["id"] = self.valid_id + 1
                        self.profit_order["order"] = self.create_order("MKT", 1, naction)#if this work, we might switch to limit
                        #execute the main order
                        self.execute_order(self.main_order["order"])
                        self.main_order["active"] = True
                        time.sleep(x_delay)
                        self.main_order["timeout"] = dt.datetime.now()
                        print "FROM SPAWN, NOT EXEC:"
                        print "main:"
                        logging.debug(str(self.main_order))
                        print self.main_order
                        print "stop:"
                        print self.stop_order
                        print "profit:"
                        print self.profit_order
                        print "CURRENT POS IS:"
                        print self.position
                    #if self.main_order["active"] and not self.main_order["filled"]:
                        #print "shelf life of main:"
                        #print (dt.datetime.now() - self.main_order["timeout"]).total_seconds()
                        #print (dt.datetime.now() - self.main_order["timeout"]).total_seconds() > self.shelflife
                    if self.main_order["active"] \
                            and not self.main_order["filled"] \
                            and (dt.datetime.now() - self.main_order["timeout"]).total_seconds() > self.shelflife:

                        self.cancel_order(self.main_order["id"])
                        time.sleep(x_delay)
                        self.main_order["active"] = False
                        print "Main order timed out"
                        logging.debug("exec - main order timed out")

                    if self.main_order["active"] and self.main_order["filled"] and not (self.stop_order["active"] or self.profit_order["active"]):
                        # print "stop/profit loop active"
                        action = self.main_order["order"].m_action
                        if action == "BUY":
                            offset = -self.stop_offset
                        if action == "SELL":
                            offset = self.stop_offset
                        if self.stop == 0:
                            self.stop = self.last_trade + offset
                        # print "stop at " + str(self.stop)
                        # print "last trade at :" +str(self.last_trade)

                        if action == "BUY":
                            self.watermark = max(self.last_trade, self.watermark)
                            # print "new watermark is:" + str(self.watermark)
                            if self.last_trade <= self.stop and not self.profit_order["active"] and not self.stop_order["active"]:
                                self.execute_order(self.stop_order["order"])
                                time.sleep(x_delay)
                                self.stop_order["active"] = True #really necessary ? I wonder
                                print "stopped out"
                            if self.flag == "trend":
                                if self.last_trade <= self.watermark + offset and not self.profit_order["active"] and not self.stop_order["active"]:
                                    self.execute_order(self.profit_order["order"])
                                    time.sleep(x_delay)
                                    self.profit_order["active"] = True
                                    print "took profits"
                        if action == "SELL":
                            self.watermark = min(self.last_trade, self.watermark)
                            if self.last_trade >= self.stop and not self.profit_order["active"] and not self.stop_order["active"]:
                                self.execute_order(self.stop_order["order"])
                                time.sleep(x_delay)
                                self.stop_order["active"] = True  # really necessary ? I wonder
                                print "stopped out"
                            if self.flag == "trend":
                                if self.last_trade >= self.watermark + offset and not self.profit_order["active"] and not self.stop_order["active"]:
                                    self.execute_order(self.profit_order["order"])
                                    time.sleep(x_delay)
                                    self.profit_order["active"] = True


                                                #for now, simple is nice
                        # print self.flag
                        # print str(abs(zscore))
                        if self.flag == "range" and abs(zscore) <= settings.Z_TARGET:
                            self.execute_order(self.profit_order["order"])
                            time.sleep(x_delay)
                            print "took range profits"
            except:
                print "trading loop crapped"
                print dt.datetime.now()


    def reset_trading_pos(self):
        self.main_order = {"id": None,
                           "order": None,
                           "timeout": None,
                           "filled": False,
                           "active": False}

        self.stop_order = {"id": None,
                           "order": None,
                           "filled": False,
                           "active": False}
        self.profit_order = {"id": None,
                             "order": None,
                             "filled": False,
                             "active": False}
        self.watermark = 0
        self.stop = 0






    def create_fill(self, msg):
        """
        Deals with fills
        """
        print "I'm looking for these ids:" + str(self.main_order["id"]) + " or " +str(self.stop_order["id"])
        print "I have this one:" + str(msg.orderId)
        # print "as-is matching:"
        # print int(msg.orderId) == self.main_order["id"]


        if len(self.fill_dict) == 0 or msg.permId != self.fill_dict[-1][4]:
            print "time to do something with the fill"
            if self.main_order["id"] == int(msg.orderId):
                self.main_order["filled"] = True
                type = "main"
                direction = self.main_order["order"].m_action
            elif self.stop_order["id"] == int(msg.orderId):
                self.stop_order["filled"] = True
                type = "stop"
                direction = self.stop_order["order"].m_action
                time.sleep(5)
                self.reset_trading_pos()
            elif self.profit_order["id"] == int(msg.orderId):
                self.profit_order["filled"] = True
                type = "profit"
                direction = self.profit_order["order"].m_action
                time.sleep(5)
                self.reset_trading_pos()#TODO I suspect something fishy here. Maybe the time.sleep will help
            else:
                print "uh, oh .. fill didn't match"
                type = "other"
                direction = "neutralize/unsure"
            print "last fill at " + str(float(msg.avgFillPrice))
            self.last_fill = [dt.datetime.now(), float(msg.avgFillPrice),type, direction, msg.permId]
            self.monitor.update_fills(self.last_fill)
            self.fill_dict.append(self.last_fill)
            #write to csv
            fd = open(self.csv, 'a')
            fd.write(dt.datetime.strftime(self.last_fill[0], format ="%Y-%m-%d %H:%M:%S") + "," + str(self.last_fill[1]) + "," + self.last_fill[2] + "," + self.last_fill[3] + "," +str(self.last_fill[4]) + "\r")
            fd.close()




    def execute_order(self, ib_order):
        """
        Execute the order through IB API
        """
        # send the order to IB
        #self.create_fill_dict_entry(self.valid_id, ib_order)
        self.ib_conn.placeOrder(
            self.valid_id, self.contract, ib_order
        )

        # order goes through!
        time.sleep(1)

        # Increment the order ID TODO not sure we need to instanciate there
        self.valid_id += 1

    def cancel_order(self,id):
        self.ib_conn.cancelOrder(id)

    def req_open(self):
        self.ib_conn.reqOpenOrders()

    def save_pickle(self):
        pickle.dump(self.fill_dict, open(os.path.join(os.path.curdir, "fills.p"),"wb"))
        #horrible code
#        pickle.dump(self.order_id, open(os.path.join(os.path.curdir, "orderid.p"), "wb"))



    def load_pickle(self):
        if os.path.exists(os.path.join(os.path.curdir, "fills.p")):

            self.fill_dict = pickle.load(open(os.path.join(os.path.curdir, "fills.p"), "rb"))
        if os.path.exists(os.path.join(os.path.curdir, "orderid.p")):
            self.order_id = pickle.load(open(os.path.join(os.path.curdir, "orderid.p"), "rb"))
        else:
            self.order_id = 1300

    def kill_em_all(self):
        self.ib_conn.reqGlobalCancel()


    def neutralize(self):

        if self.position > 0:
            neut = self.create_order("MKT",abs(self.position),"SELL")
        if self.position < 0:
            neut = self.create_order("MKT", abs(self.position), "BUY")
        self.execute_order(neut)
        time.sleep(1)

        self.ib_conn.reqGlobalCancel()


    def pass_position(self):
        return self.position

#TODO register on HFT class
    def on_tick_event(self, msg):
        ticker_id = msg.tickerId
        field_type = msg.field
        #        print field_type

        # Store information from last traded price
        if field_type == datatype.FIELD_LAST_PRICE:
            # print "tick"
            last_price = msg.price
            # self.__add_market_data(ticker_id, dt.datetime.now(self.tz), last_price, 1)
            # self.last_trade = last_price  # TODO this could be obsolete
            self.mkt_data_queue.put(
                dict(time=dt.datetime.now(), type="last_trade", value=float(last_price)))
        if field_type == datatype.FIELD_LAST_SIZE:
            pass
            # last_size = msg.size
            # self.__add_market_data(ticker_id, dt.datetime.now(self.tz), last_size, 2)

        if field_type == datatype.FIELD_ASK_PRICE:
            ask_price = msg.price
            # self.__add_market_data(ticker_id, dt.datetime.now(self.tz), ask_price, 3)
            # self.last_ask = ask_price
            self.mkt_data_queue.put(
                dict(time=dt.datetime.now(), type="ask_price", value=float(ask_price)))
        if field_type == datatype.FIELD_ASK_SIZE:
            pass
            # ask_size = msg.size
            # self.__add_market_data(ticker_id, dt.datetime.now(self.tz), ask_size, 4)
        if field_type == datatype.FIELD_BID_PRICE:
            bid_price = msg.price
            # self.__add_market_data(ticker_id, dt.datetime.now(self.tz), bid_price, 5)
            # self.last_bid = bid_price
            self.mkt_data_queue.put(
                dict(time=dt.datetime.now(), type="bid_price", value=float(bid_price)))
        if field_type == datatype.FIELD_BID_SIZE:
            # bid_size = msg.size
            pass
        # self.__add_market_data(ticker_id, dt.datetime.now(self.tz), bid_size, 6)


    def on_tick_generic(self,msg):

        print msg

            #print "trade " + str(self.last_trade)
        # if field_type == 1:
        #     self.last_ask = float(msg.price)
        #     print "ask " + str(self.last_ask)

        # if field_type == 2:
        #     self.last_bid = float(msg.price)
        #     print "bid" + str(self.last_bid)

    def queue_tester(self):
        for message in [dict(time=dt.datetime(2016, 5, 4, 12, 0, 0), type="ask", value= 40),
        dict(time=dt.datetime(2016, 5, 4, 12, 0, 5), type="ask", value= 41),
        dict(time=dt.datetime(2016, 5, 4, 12, 0, 0), type="ask", value= 40)]:
            self.mkt_data_queue.put(message)
            time.sleep(0.1)




            # if self.mkt_data_queue.empty():
            #     break


########################
# Monitor is actually useless and this passpass BS is an issue
########################


class Monit_stream:

    def __init__(self):
        #authenticate using settings
        tls.set_credentials_file(username=settings.PLOTLY_USER, api_key=settings.PLOTLY_API)
        tls.set_credentials_file(stream_ids=settings.PLOTLY_STREAMS)
        self.credentials = tls.get_credentials_file()['stream_ids']



# Get stream id from stream id list
#stream_id = stream_ids[0]

# Make instance of stream id object
#stream = Stream(
#    token=stream_id,  # (!) link stream id to 'token' key
#    maxpoints=80      # (!) keep a max of 80 pts on screen
#)
# Init. 1st scatter obj (the pendulums) with stream_ids[1]
        self.prices = Scatter(
            x=[],  # init. data lists
            y=[],
            mode='lines+markers',    # markers at pendulum's nodes, lines in-bt.
              # reduce opacity
            marker=Marker(size=1),  # increase marker size
            stream=Stream(token=self.credentials[0], maxpoints=2000)  # (!) link stream id to token
            )

# Set limits and mean, but later
        self.limit_up = Scatter(
            x=[],  # init. data lists
            y=[],
            mode='lines',                             # path drawn as line
            line=Line(color='rgba(31,119,180,0.15)'), # light blue line color
            stream=Stream(
            token=self.credentials[1], maxpoints=2000         # plot a max of 100 pts on screen
            )
            )
        self.limit_dwn = Scatter(
            x=[],  # init. data lists
            y=[],
            mode='lines',                             # path drawn as line
            line=Line(color='rgba(31,119,180,0.15)'), # light blue line color
            stream=Stream(
            token=self.credentials[2], maxpoints=2000# plot a max of 100 pts on screen
            )
            )
        self.ranging = Scatter(
            x=[],  # init. data lists
            y=[],
            mode='markers',
            line=Line(color='rgba(200,0,0,0.5)'), # red if the system thinks it ranges
              # reduce opacity
            marker=Marker(size=5),  # increase marker size
            stream=Stream(token=self.credentials[3], maxpoints=1000)
            )

        self.fills_buy = Scatter(
            x=[],  # init. data lists
            y=[],
            mode='markers',

            marker=Marker(size=15, color='rgba(76,178,127,0.7)'),  # increase marker size
            stream=Stream(token=self.credentials[4], maxpoints=10)
        )
        self.fills_sell = Scatter(
            x=[],  # init. data lists
            y=[],
            mode='markers',

            marker=Marker(size=15, color='rgba(178,76,76,0.7)'),  # increase marker size
            stream=Stream(token=self.credentials[5], maxpoints=10)
        )
# (@) Send fig to Plotly, initialize streaming plot, open tab
        self.stream1 = py.Stream(self.credentials[0])

# (@) Make 2nd instance of the stream link object,
#     with same stream id as the 2nd stream id object (in trace2)
        self.stream2 = py.Stream(self.credentials[1])
        self.stream3 = py.Stream(self.credentials[2])
        self.stream4 = py.Stream(self.credentials[3])
        self.stream5 = py.Stream(self.credentials[4])
        self.stream6 = py.Stream(self.credentials[5])
# data
        self.data = Data([self.prices,self.limit_up,self.limit_dwn,self.ranging, self.fills_buy, self.fills_sell])
# Make figure object
        self.layout = Layout(showlegend=False)
        self.fig = Figure(data=self.data, layout=self.layout)
        self.unique_url = py.plot(self.fig, filename='Azure-IB Monitor', auto_open=False)
# (@) Open both streams
        self.stream1.open()
        self.stream2.open()
        self.stream3.open()
        self.stream4.open()
        self.stream5.open()
        self.stream6.open()
        print "streams initaited"

    def update_data_point(self,msg,last_mean,last_sd,flag):
        """
        now based on a dict input, from the parser
        :param last_price:
        :param last_mean:
        :param last_sd:
        :param flag:
        :return:
        """
        now = msg["time"]
        last_price = msg["value"]
        self.stream1.write(dict(x=now, y=last_price))
        self.stream2.write(dict(x=now, y=last_mean+settings.Z_THRESH*last_sd))
        self.stream3.write(dict(x=now, y=last_mean-settings.Z_THRESH*last_sd))

        if flag == "range":
            self.stream4.write(dict(x=now, y=last_price))

    def update_fills(self, fill):
        #now=dt.datetime.now()
        if fill is not None:
            if fill[3] == "BUY":
                self.stream5.write(dict(x=fill[0], y=fill[1]))

            if fill[3] == "SELL":
                self.stream6.write(dict(x=fill[0], y=fill[1]))

    def close_stream(self):
        self.stream1.close()
        self.stream2.close()
        self.stream3.close()
        self.stream4.close()
        self.stream5.close()
        # (@) Write 1 point corresponding to 1 pt of path,
        #     appending the data on the plot




def message_tester(queue):
    while not queue.empty():
    #     msg = queue.get()
    #     logging.DEBUG("consumer did something")
        print queue.get()
        if queue.empty():
            break
######
#ALLTHIS ISSCAFFOLDING TOTEST THE ORDER LOGIC

if __name__ == "__main__":
#register Ib connection

    def reply_handler(msg):
        print("Reply:", msg)

    model_conn=ibConnection(host="localhost",port=4001, clientId=130)
    #
    model_conn.connect()

    #base scaffolding
    test = ExecutionHandler(model_conn, test=False)
    model_conn.registerAll(test._reply_handler)
    # model_conn.unregister(ib_message_type.tickPrice)
    model_conn.register(test.on_tick_event, ib_message_type.tickPrice)
    # model_conn.reqPositions()
    #die sequence

    #test sequence
    # time.sleep(2)
    # print "initial validid print"
    # if test.valid_id is None:
    #     test.valid_id = 1900
    # print test.valid_id
    # print test.position
    # test.neutralize()
    model_conn.reqMktData(1,test.contract,'',False)
    # time.sleep(2)
    # model_conn.reqGlobalCancel()
    # model_conn.cancelPositions()
    # print test.last_fill



    test.cur_mean =44.1
    test.cur_sd = 0.01
    test.flag = "range"
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    parser = executor.submit(test.queue_parser)
    trader = executor.submit(test.trading_loop)
        # messenger2  =executor.submit(message_tester, test.mkt_data_queue)
    time.sleep(30)
    print test.prices
    #test die sequence - OK!
    # print "die sequence:"
    # test.on_tick(2,20,test.last_trade,"trend",0)
    # time.sleep(1)
    # test.on_tick(2,20,test.last_trade,"trend",0)
    # time.sleep(1)
    # test.on_tick(2,20,test.last_trade,"trend",0)
    # time.sleep(4)
    # test.on_tick(2, 20, test.last_trade, "trend", 0)
    #
    #initiate trend trading sequence with stop - OK!
    # print "stop test sequence"
    # test.on_tick(2,test.last_bid,test.last_trade,"trend",10)
    # time.sleep(1)
    # test.main_order["filled"] = True
    # print "I am:"
    # print test.main_order["order"].m_action
    # test.on_tick(0,0,0,"trend",test.last_trade - 0.5)

    #initiate trend trading sequence with profit - OK
    # print "profit test sequence"
    # test.on_tick(2,test.last_bid,test.last_trade,"trend",10)
    # time.sleep(1)
    # print test.main_order["order"]
    # test.main_order["filled"] = True
    # print "I am:" + test.main_order["order"].m_action
    # print  "main is filled:" + str(test.main_order["filled"])
    #
    # test.on_tick(0,0,0,"trend",10.2)
    # time.sleep(0.5)
    # test.on_tick(0,0,0,"trend",10.5)
    # time.sleep(0.5)
    # test.on_tick(0, 0, 0, "trend", 10.4)
    # time.sleep(4)

    #initiate range with stop - OK!

    # print " range stop test sequence"
    # test.on_tick(2,test.last_bid,test.last_trade,"range",10)
    # time.sleep(1)
    # test.main_order["filled"] = True
    # print "I am:"
    # print test.main_order["order"].m_action
    # test.on_tick(0,0,0,"trend", 9.9)
    # time.sleep(0.5)
    # test.on_tick(0,0,0,"trend", 9.8)
    # test.on_tick(0,0,0,"trend", 10.3)
    # time.sleep(4)

    #initiate range with profit - OK

    # print " range profit test sequence"
    # test.on_tick(2,test.last_bid,test.last_trade,"range",10)
    # time.sleep(1)
    # test.main_order["filled"] = True
    # print "I am:"
    # print test.main_order["order"].m_action
    # test.on_tick(1.5,0,0,"range", 9.9)
    # time.sleep(0.5)
    # test.on_tick(1,0,0,"range", 9.8)
    # time.sleep(0.5)
    # test.on_tick(0.1,0,0,"range", 9)
    # time.sleep(4)

    #initiate change state test- OK!

    # print " change state test sequence"
    # test.on_tick(2,test.last_bid,test.last_trade,"trend",10)
    # time.sleep(1)
    # test.on_tick(2,test.last_bid,test.last_trade,"range",10)
    # time.sleep(2)



    #conclude by checking orders are reset
    #print test.main_order
    #print test.stop_order
    #print test.profit_order
    #test.reset_trading_pos()



    # for key in test.fill_dict:
    #     print key
    #test.save_pickle()
    model_conn.disconnect()
