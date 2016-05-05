
import datetime as dt
import time
import pickle
import os


from ib.ext.Contract import Contract
from ib.ext.Order import Order

from ib.opt import ibConnection, message as ib_message_type


class ExecutionHandler(object):
    """
    Handles order execution via the Interactive Brokers API
    """

    def __init__(self, ib_conn):
        # initialize
        self.ib_conn = ib_conn
        self.valid_id = None
        self.position = None
        self.contract = self.create_contract("CL",'FUT', 'NYMEX', '201606','USD')
        self.is_trading = False
        #will need  a test for pickle existence TODO panda the pickle or something
        self.zscore = None
        self.zscore_thresh = 2
        self.thresh_tgt = 0
        self.flag = None
        self.hist_flag = None
        self.main_order = {"id": None,
                           "order": None,
                           "timeout": None,
                           "filled": None,
                           "active": False}
        self.stop_order = {"id": None,
                           "order": None,
                           "filled": None,
                           "active": False}
        self.profit_order ={"id": None,
                           "order": None,
                           "filled": None,
                           "active": False}
        #probably unnecessary if not used as __main__
        self.last_trade = None
        self.last_bid = None
        self.last_ask = None
        self.last_fill = None
        self.watermark = None
        self.stop_offset = 0.04
        self.shelflife = 5




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
                #hackish attempt to pass stuff to the plotly monitor
                print "last fill at " + str(float(msg.avgFillPrice))
                self.last_fill = [dt.datetime.now(), float(msg.avgFillPrice)]

    def passpass(self):

        if self.last_fill is not None:
            return self.last_fill
            self.last_fill = None



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

    def on_tick(self,zscore,cur_bid,cur_ask, cur_flag, cur_trade):
        self.zscore = zscore
        self.last_bid = cur_bid
        self.last_ask = cur_ask
        self.last_trade = cur_trade
        self.flag =cur_flag
        #first, checkzscore and do an order
        if abs(self.zscore) >= self.zscore_thresh and not self.main_order["active"]: #need to check for other status

            if self.zscore >= self.zscore_thresh:
                if self.flag == "trend":
                    action = "BUY"

                if self.flag == "range":
                    action = "SELL"

            if self.zscore <= -self.zscore_thresh:
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
            self.main_order["timeout"] = dt.datetime.now()

        if self.main_order["active"] \
                and not self.main_order["filled"] \
                and dt.datetime.now() > self.main_order["timeout"]+ dt.timedelta(seconds=self.shelflife):
            self.cancel_order(main_order["id"])
            self.main_order["active"] = False
            print "Main order timed out"

        if self.main_order["active"] and self.main_order["filled"]:
            stop = self.last_trade + offset

            if action == "BUY":
                watermark = max(self.last_trade, watermark)
                if self.last_trade <= stop:
                    self.execute_order(self.stop_order)
                    self.stop_order["active"] = True #really necessary ? I wonder
                    print "stopped out"
                if self.flag == "trend":
                    if self.last_trade <= watermark + offset:
                        self.execute_order(self.profit_order)
                        self.profit_order["active"] = True
            if action == "SELL":
                watermark = min(self.last_trade, watermark)
                if self.last_trade >= stop:
                    self.execute_order(self.stop_order)
                    self.stop_order["active"] = True  # really necessary ? I wonder
                    print "stopped out"
                if self.flag == "trend":
                    if self.last_trade >= watermark + offset:
                        self.execute_order(self.profit_order)
                        self.profit_order["active"] = True


                                    #for now, simple is nice
            if self.flag == "range" and abs(self.zscore) <= 0.2:#TODO hardcoded is not smart
                self.execute_order(self.profit_order)

    def reset_trading_pos(self):
        self.main_order = {"id": None,
                           "order": None,
                           "timeout": None,
                           "filled": None,
                           "active": False}

        self.stop_order = {"id": None,
                           "order": None,
                           "filled": None,
                           "active": False}
        self.profit_order = {"id": None,
                             "order": None,
                             "filled": None,
                             "active": False}




    def create_fill_dict_entry(self, id, order):
        """
        Creates an entry in the Fill Dictionary that lists
        orderIds and provides security information. This is
        needed for the event-driven behaviour of the IB
        server message behaviour.
        """
        # self.fill_dict[id] = {
        #     "timestamp": dt.datetime.now(),
        #     "order": order.m_orderType,
        #     "direction": order.m_action,
        #     "filled": False,
        #     "price": 0
        # }
        print "created entry"

    def create_fill(self, msg):
        """
        Places a fill in the fill dictionary
        """
        if self.main_order["id"] == int(msg.orderId):
            self.main_order["filled"] = True
        if self.stop_order["id"] == int(msg.orderId):
            self.stop_order["filled"] = True
            self.reset_trading_pos()
        if self.profit_order["id"] == int(msg.orderId):
            self.profit_order["filled"] = True
            self.reset_trading_pos()
        else:
            print "uh, oh .. fill didn't match"
        print "main:"
        print self.main_order
        print "stop:"
        print self.stop_order
        print "profit:"
        print self.profit_order

        # Check for fill and no duplicates
        #self.fill_dict[msg.orderId]["filled"] = True
        #self.fill_dict[msg.orderId["price"] = msg.


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
        if self.position !=0:
            if self.position > 0:
                neut = self.create_order("MKT",self.position,"SELL")
            if self.position < 0:
                neut = self.create_order("MKT", self.position, "BUY")
            self.execute_order(neut)

    def pass_position(self):
        return  self.position

#scaffolding tick data management for testing purposes
    def on_tick_event(self, msg):
        ticker_id = msg.tickerId
        field_type = msg.field
        #        print field_type

        # Store information from last traded price
        if field_type == 4:
            self.last_trade = float(msg.price)


            #print "trade " + str(self.last_trade)
        if field_type == 1:
            self.last_ask = float(msg.price)
        #print "ask " + str(self.last_ask)

        if field_type == 2:
            self.last_bid = float(msg.price)
            #print "bid" + str(self.last_bid)
        self.on_tick(2,self.last_bid,self.last_trade,"trend",self.last_trade)


######
#ALLTHIS ISSCAFFOLDING TOTEST THE ORDER LOGIC

if __name__ == "__main__":
#register Ib connection

    def reply_handler(msg):
        print("Reply:", msg)

    model_conn=ibConnection(host="localhost",port=4001, clientId=130)

    model_conn.connect()

    #base scaffolding
    test = ExecutionHandler(model_conn)
    model_conn.registerAll(test._reply_handler)
    model_conn.unregister(ib_message_type.tickPrice)
    model_conn.register(test.on_tick_event, ib_message_type.tickPrice)
    model_conn.reqPositions()
    model_conn.reqGlobalCancel()
    model_conn.cancelPositions()

    #test sequence
    time.sleep(2)
    print "initial validid print"
    print test.valid_id
    print test.position
    test.neutralize()
    model_conn.reqMktData(1,test.contract,'',False)
    time.sleep(1)
    #test place trade

    # test.execute_order(test.contract,test.create_order("MKT",1,"BUY"))
    # time.sleep(5)
    # test.execute_order(test.contract,test.create_order("MKT",1,"SELL"))
    # time.sleep(5)
    #test.place_trade("BUY", test.last_bid,test.last_ask)
    time.sleep(20)
    # for key in test.fill_dict:
    #     print key
    #test.save_pickle()
    model_conn.disconnect()
