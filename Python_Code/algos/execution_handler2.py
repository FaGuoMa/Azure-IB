
import datetime as dt
import time
import pickle
import os
from multiprocessing import Queue, Event
import concurrent.futures
import sys
import ib.ext
from ib.ext.Contract import Contract
from ib.ext.Order import Order
import traceback

from ib.opt import ibConnection, Connection, message as ib_message_type
# from ib.ext.EWrapper import EWrapper
#import logging
import pandas as pd
from params import settings
import logging
from csv import DictWriter
import plotly.plotly as py
import plotly.tools as tls
from plotly.graph_objs import *
import datetime as dt
from params import settings
import params.ib_data_types as datatype


class Trade:
    def __init__(self,msg, queue_ack, fill_dict, ib_conn, contract,valid_id):
        self.t_signal = msg["t_signal"]
        self.t_x = None
        self.t_ack = None
        self.signal_at = msg["signal_at"]
        self.type = msg["type"]
        self.action = msg["action"]
        self.naction = None
        self.id = msg["id"]
        self.cntdwn = 3#todo variable static
        self.timedout = False
        self.flag = msg["flag"]
        self.fill_price = None
        self.filled = False
        self.fill_dict = fill_dict
        print "initate IB contract and order"
        self.conn = ib_conn
        self.contract = contract
        self.last_bid = msg["last_bid"]
        self.last_ask = msg["last_ask"]
        self.order = None
        self.valid_id = valid_id



        if self.action == "BUY":
            print "make buy order"
            self.naction = "SELL"
        if self.action == "SELL":
            print "make sell order"
            self.naction = "BUY"

        logging.basicConfig(format='%(asctime)s %(message)s')
        self.exec_logger = logging.getLogger('execution_logger')
        self.exec_logger.setLevel(logging.INFO)






    def  __coerce__(self, other):
        return dict(trend=self.flag,
                    t_signal = self.t_signal,
                    t_x=self.t_x,
                    t_ack=self.t_ack,
                    signal_at=self.signal_at,
                    type=self.type,
                    action=self.action,
                    id=self.id,
                    cntdwn=self.cntdwn,
                    flag=self.flag)

    def execute(self, executing_flag):
        if executing_flag.is_set():
            print "build order"
            self.order = Order()
            self.order.m_orderType = "LMT"
            self.order.m_totalQuantity = 1#todo hardcoded is not nice
            if self.type == "main":
                self.order.m_action = self.action
            else:
                self.order.m_action = self.naction
            if self.action == "BUY":
                self.order.m_lmtPrice = self.last_bid
            elif self.action == "SELL":
                self.order.m_lmtPrice = self.last_ask
            print "push order"
            try:
                self.conn.placeOrder(self.valid_id, self.contract, self.order)
                time.sleep(1)
            except:
                self.exec_logger.error("place order failed - Trade")
                traceback.print_exc()

            self.t_x = dt.datetime.now()



    def change(self,type):
        print "change " +str(self.id)+"order  to " + type
        if type == "CL":
            self.conn.cancelOrder(self.valid_id)
            time.sleep(1)
        else:
            self.order.m_orderType = type
            self.conn.placeOrder(self.valid_id,self.contract,self.order)
            time.sleep(1)

    def timeout(self):
        if dt.datetime.now() > self.t_x + dt.timedelta(seconds=self.cntdwn):
            return False
        else:
            return True

        if self.type == "main":
            self.change("LMT","MKT")

    def return_dict(self):
        return dict(t_signal=self.t_signal,
             t_x=self.t_x,
             t_ack=self.t_ack,
             signal_at=self.signal_at,
             type=self.type,
             action=self.action,
             id=self.id,
             flag=self.flag,
             filled = self.filled,
             fill_price = self.fill_price,
             )

    def wait_for_ack(self, queue_ack, executing_flag, csv):

        print "looking for:"
        print self.type
        while True:

            if self.t_x + dt.timedelta(seconds=self.cntdwn) < dt.datetime.now():
                self.timedout = True
                if self.type == "main":

                    print "change order to CL"
                    self.change("CL")
                    executing_flag.set()
                    return False
                if self.type == "stop" or self.type == "profit":
                    print "change order from LMT to MKT"
                    self.change("MKT")

            if not queue_ack.empty():
                ack = queue_ack.get()

                if ack["head"] == "open":
                    if ack["id"] == self.id:
                        print "confirmed id: " + str(ack["id"])
                        if ack["action"] == self.action:
                            print "confirmed direction:" + ack["action"]
                        else:
                            print "error with open order direction for the same if"
                elif ack["head"] == "status":
                    print ack
                    if self.fill_dict != []:
                        duplicate_condition = False
                        if ack["id"] == self.fill_dict[-1]["id"]:
                            duplicate_condition = True
                    else:
                        duplicate_condition = False

                    if ack["id"] == self.id and not duplicate_condition:
                        print "confirmed ack id: " + str(ack["id"])
                        if ack["fill"] != 0 and (self.fill_dict == [] or self.fill_dict[-1]["id"] != ack["id"]):
                            self.fill_price = ack["fill_price"]
                            self.t_ack = dt.datetime.now()
                            self.filled = True

                            self.fill_dict.append(self.return_dict())

                            try:
                                fd = open(csv, 'a')
                                fieldnames = ["t_signal", "t_x", "t_ack", "signal_at", "flag", "fill_price", "action",
                                              "type", "id", "filled"]
                                writer = DictWriter(fd, fieldnames=fieldnames)
                                writer.writerow(self.fill_dict[-1])
                                fd.close()

                            except:
                                self.exec_logger.error("filled dict failed - exec")  # todo this will fail on cancel
                            executing_flag.set()
                            return True
                    else:
                        print "check fill dict"
                        print [d["id"] for d in self.fill_dict]
                        if ack["id"] in [d["id"] for d in self.fill_dict]:
                            print "redundant ack from previous order"
                        else:
                            print "ID number for ack is unknown !!!!"



class ExecutionHandler(object):
    """
    Handles order execution via the Interactive Brokers API
    """

    def __init__(self, ib_conn, test=False):
        # initialize
        self.test = test
        self.ib_conn = ib_conn
        self.valid_id = None
        self.position = None
        self.contract = self.create_contract(settings.SYMBOL,settings.SECURITY,settings.EXCHANGE,settings.EXPIRY,settings.CURRENCY)
        self.zscore = None
        self.zscore_thresh = settings.Z_THRESH
        self.thresh_tgt = 0
        self.flag = None
        self.hist_flag = None
        self.trade = None
        self.mkt_data_queue = Queue()
        self.message_q = Queue()
        self.order_q = Queue()
        self.trading = Event()
        self.trading.set()
        self.executing = Event()
        self.executing.set()
        self.last_trade = None#todo this is not thread safe
        self.last_bid = None
        self.last_ask = None
        self.last_fill = None
        self.cur_mean = None
        self.cur_sd = None
        self.flag = None
        if not self.test:
            self.monitor = Monit_stream()
        self.watermark = 0
        self.stop_offset = settings.STOP_OFFSET
        self.stop = 0
        self.shelflife = 3
        self.fill_dict = []
        self.csv = self.data_path = os.path.normpath(os.path.join(os.path.curdir,"fills.csv"))
        self.prices = pd.DataFrame(columns=["price"])
        logging.basicConfig(format='%(asctime)s %(message)s')
        self.exec_logger = logging.getLogger('execution_logger')
        self.exec_logger.setLevel(logging.INFO)






    def _reply_handler(self, msg):
        if msg.typeName == "nextValidId" and self.valid_id is None:
            self.valid_id =int(msg.orderId)

        if msg.typeName == "position":
            self.position = int(msg.pos)

        if msg.typeName == "openOrder":
            print msg
            self.message_q.put(dict(head="open",
                                    id=msg.orderId,
                                    action=msg.order.m_action))
        if msg.typeName == "orderStatus":
            print msg
            self.message_q.put(dict(head="status",
                                    id=msg.orderId,fill=msg.filled,
                                    fill_price=msg.avgFillPrice))

        if msg.typeName == "error":
            print "error intercepted"
            print msg


    def opp_action(self,action):
        if action == "BUY":
            return "SELL"
        if action == "BUY":
            return "SELL"


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

        while True:

            msg = self.mkt_data_queue.get()
            if msg["type"] == "last_trade":

                self.last_trade = msg["value"]
                self.prices.loc[msg['time'],"price"] = msg["value"]


                #self.prices.append()
                # print "test pandas"
                # print str(self.cur_mean)
                # print str(self.cur_sd)
                # print str(self.flag)
                #print msg["value"]
                if self.cur_mean is not None  and self.cur_sd is not None and self.flag is not None:
                    try:
                     self.monitor.update_data_point(msg,self.cur_mean,self.cur_sd,self.flag)
                    except:
                        self.exec_logger.error("update plotly failed - parser")
                        traceback.print_exc()


            if msg["type"] == "ask_price":
                self.last_ask = msg["value"]

            if msg["type"] == "bid_price":
                self.last_bid = msg["value"]

    def make_order_q_message(self,type,action):
        dict_message = dict(t_signal=dt.datetime.now(),
                    t_x= None,
                    t_ack= None,
                    signal_at=self.last_trade,
                    type=type,
                    action=action,
                    id=self.valid_id,
                    flag=self.flag,
                    filled=False,
                    fill_price=None,
                    last_ask = self.last_ask,
                    last_bid = self.last_bid

                    )

        self.order_q.put(dict_message)

    def trading_loop(self):
        """
        So, this one will not take any outside input, but loop forever instead. *Should be thread-safe*

        """

        x_delay = 0.5#todo not sure if still required
        while True:
            try:
                self.trading.wait()

                if self.cur_mean is not None and self.cur_sd is not None and self.trading.is_set():

                    try:
                        zscore = (self.last_trade - self.cur_mean)/self.cur_sd
                    except:
                        self.exec_logger.error("zscore update failed - strategy")

                    if self.hist_flag is None:
                        self.hist_flag = self.flag
                        self.exec_logger.error("set hist flag in trading loop - strategy")
                    #check change of state and kill positions TODO this is simplistic
                    # try:
                    #     if self.hist_flag != self.flag and self.trade is not None and self.trade.type == "main":
                    #         self.exec_logger.error("killing pos for change of flag - strategy")
                    #         #logging.debug("exec - change of state, killing position")
                    #         self.make_order_q_message("stop",self.opp_action(self.fill_dict[-1]["action"]))
                    #
                    #         #self.execute_order(self.stop_order["order"])
                    #         #self.make_order_q_message("stop",naction) todo not sure this goes here
                    #         self.hist_flag = self.flag
                    #
                    # except:
                    #     self.exec_logger.error("market in change of trend failed - strategy")

                    if abs(zscore) >= self.zscore_thresh and \
                                    abs(zscore) <= settings.Z_THRESH + settings.Z_THRESH_UP and \
                                    self.trading.is_set() and \
                            (self.fill_dict == [] or self.fill_dict[-1]["type"] != "main") and \
                                    self.flag != "nothing":
                        self.exec_logger.info("signal for main detected - strategy")
                        try:

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
                                naction = self.opp_action(action)
                                #price = self.last_bid  todo manage bid/ask in execution
                                offset = -self.stop_offset
                            if action == "SELL":
                                naction = self.opp_action(action)
                                #price = self.last_ask
                                offset = self.stop_offset

                            #spawn main order, stop and profit
                            #self.main_order["id"] = self.valid_id
                            #self.main_order["order"] = self.create_order("LMT",1,action,price)
                            #self.stop_order["id"] = self.valid_id + 1
                            #self.stop_order["order"] = self.create_order("MKT", 1, naction)
                            #self.profit_order["id"] = self.valid_id + 1
                            #self.profit_order["order"] = self.create_order("MKT", 1, naction)#if this work, we might switch to limit
                            #execute the main order
                            #self.main_order["active"] = True
                            try:
                                self.make_order_q_message("main", action)
                                time.sleep(x_delay)

                            except:
                                self.exec_logger.error("push main order to queue failed - strategy")

                            time.sleep(x_delay)

                            print "CURRENT POS IS:"
                            print self.position
                        except:
                            self.exec_logger.error("main order failed - strategy")

                    if self.trading.is_set() and \
                                    len(self.fill_dict) >0 and \
                                    self.fill_dict[-1]["type"] == "main" \
                            and self.fill_dict[-1]["filled"]:

                        action = self.fill_dict[-1]["action"]
                        if action == "BUY":
                            offset = -self.stop_offset
                            naction = "SELL"
                        if action == "SELL":
                            offset = self.stop_offset
                            naction = "BUY"
                        if self.stop == 0:
                            self.stop = self.fill_dict[-1]["fill_price"] + offset

                        if action == "BUY":

                            try:
                                self.watermark = max(self.last_trade, self.watermark)
                                if self.last_trade <= self.stop and self.trading.is_set():
                                    self.exec_logger.error("executing stop order buy- strategy")
                                    self.make_order_q_message("stop",naction)
                                    time.sleep(x_delay)
                            except:

                                self.exec_logger.error("buy stop failed - strategy")

                            if self.flag == "trend":
                                try:
                                    if self.last_trade <= self.watermark + offset and self.trading.is_set():
                                        self.make_order_q_message("profit",naction)
                                        self.exec_logger.error("executing profit order buy/trends - strategy")
                                        time.sleep(x_delay)
                                except:
                                    self.exec_logger.error("buy profit failed - strategy")


                        if action == "SELL":
                            try:
                                self.watermark = min(self.last_trade, self.watermark)
                                if self.last_trade >= self.stop and self.trading.is_set():

                                    self.make_order_q_message("stop",naction)
                                    time.sleep(x_delay)
                                    self.exec_logger.error("executing stop order sell- strategy")
                            except:

                                self.exec_logger.error("sell stop failed - strategy")

                            if self.flag == "trend":
                                try:
                                    if self.last_trade >= self.watermark + offset and self.trading.is_set():
                                        self.make_order_q_message("profit",naction)

                                        self.exec_logger.error("executing profit order sell/trend - strategy")
                                        time.sleep(x_delay)
                                except:
                                    self.exec_logger.error("sell profit failed - strategy")



                        if self.flag == "range" and abs(zscore) <= settings.Z_TARGET and self.trading.is_set():
                            try:

                                self.make_order_q_message("profit",naction)
                                self.exec_logger.error("executing profit order, range mean revert target - strategy")
                                time.sleep(x_delay)

                            except:
                                self.exec_logger.error("range profit failed - strategy")
            except:
                traceback.print_exc()

    def order_execution(self):
        while True:

            if not self.order_q.empty() and self.executing.is_set():
                msg = self.order_q.get()
                self.trading.clear()
                self.flush_q_orders()


                self.exec_logger.info("order execution thread - got msg")
                try:
                    self.trade = Trade(msg, self.message_q, self.fill_dict, self.ib_conn,self.contract,self.valid_id)
                    print "order exec: executing {0} order at {1}".format(msg["action"], msg["signal_at"])

                    self.trade.execute(self.executing)
                    self.executing.clear()



                except:
                    self.exec_logger.error("execute order failed - Trade")
                    traceback.print_exc()
                try:
                    self.trade.filled = self.trade.wait_for_ack(self.message_q, self.executing, self.csv)
                except:
                    self.exec_logger.error("wait for ack failed - Trade")
                    traceback.print_exc()

                print self.fill_dict

                try:
                    if self.trade.type != "main" or self.trade.timedout:
                        self.reset_trading_pos()

                except:
                    self.exec_logger.error("monitor update fill failed - exec")
                try:
                    if self.trade.filled:
                        self.monitor.update_fills(self.fill_dict)
                except:
                    self.exec_logger.error("monitor update fill failed - exec")




                self.flush_q_orders()
                self.valid_id += 1
                self.executing.wait()
                self.trading.set()
                self.trade = None

    def flush_q_orders(self):
        while not self.order_q.empty():
            self.order_q.get_nowait()

    def reset_trading_pos(self):
        self.current_order = {"main": True,
                           "action": None,

                           "filled": False,
                           "active": False}


        self.watermark = 0
        self.stop = 0
        self.stop_profit = None
        self.exec_logger.error("reseted trading state - reset")





    def create_fill(self, msg):
        """
        Deals with fills
        """
        if self.main_order["order"] is not None or self.stop_order["order"] is not None or self.profit_order["order"] is not None:
            print "I'm looking for these ids:" + str(self.main_order["id"]) + " or " +str(self.stop_order["id"]) +" or "+str(self.profit_order["id"])
            print "I have this one:" + str(msg.orderId)
            # print "as-is matching:"
            # print int(msg.orderId) == self.main_order["id"]


            if len(self.fill_dict) == 0 or msg.permId != self.fill_dict[-1][4]:

                if self.main_order["id"] == int(msg.orderId):
                    self.main_order["filled"] = True
                    type = "main"
                    direction = self.main_order["order"].m_action
                elif self.stop_order["id"] == int(msg.orderId):
                    self.stop_order["filled"] = True
                    type = "stop"
                    direction = self.stop_order["order"].m_action
                    time.sleep(1)
                    self.reset_trading_pos()
                elif self.profit_order["id"] == int(msg.orderId):
                    self.profit_order["filled"] = True
                    type = "profit"
                    direction = self.profit_order["order"].m_action
                    time.sleep(1)
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
        print msg




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

        # Increment the order ID
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
        if  self.position is not None:
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
        #print "tick"
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
        #print str(last_mean)
        #print str(last_sd)
        try:
            now = msg["time"]
            last_price = msg["value"]
        except Exception as e:
            print "msg capture error" + e

        try:
            # self.exec_logger.error("last fill" + str(last_mean))
            # self.exec_logger.error("last fill" + str(last_sd))
            self.stream1.write(dict(x=now, y=last_price))
            self.stream2.write(dict(x=now, y=last_mean+settings.Z_THRESH*last_sd))
            self.stream3.write(dict(x=now, y=last_mean-settings.Z_THRESH*last_sd))
            # self.exec_logger.error("last fill" + str(last_mean))
            # self.exec_logger.error("last fill" + str(last_sd))
        except Exception as e:
            print e + "streams failed"
        if flag == "range":
            self.stream4.write(dict(x=now, y=last_price))

    def update_fills(self, fill_dict):
        #now=dt.datetime.now()
        if fill_dict is not None:
            if fill_dict[-1]["action"] == "BUY":
                self.stream5.write(dict(x=fill_dict[-1]["t_ack"], y=fill_dict[-1]["fill_price"]))

            if fill_dict[-1]["action"] == "SELL":
                self.stream6.write(dict(x=fill_dict[-1]["t_ack"], y=fill_dict[-1]["fill_price"]))

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
    test = ExecutionHandler(model_conn, test=True)
    model_conn.registerAll(test._reply_handler)
    # model_conn.unregister(ib_message_type.tickPrice)
    model_conn.register(test.on_tick_event, ib_message_type.tickPrice)
    # model_conn.reqPositions()


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

    # die sequence

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    #parser = executor.submit(test.queue_parser)
    trader = executor.submit(test.trading_loop)
    order = executor.submit(test.order_execution)

    test.flag = "range"
    test.hist_flag = "range"
    test.last_trade = 40.0
    test.cur_mean = 38.0
    test.cur_sd = 0.9
    time.sleep(0.1)
    test.message_q.put(dict(head="open",id=1,action="BUY"))
    test.message_q.put(dict(head="status", id=test.valid_id,fill=1,fill_price=40.0))

    print "order alive"
    print order.running()
    print "order queue empty"
    print test.order_q.empty()
    print "trader alivee"
    print trader.running()

    time.sleep(5)
    print test.prices
    #test die sequence - OK!


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
    print "END OF TEST"
    model_conn.disconnect()
