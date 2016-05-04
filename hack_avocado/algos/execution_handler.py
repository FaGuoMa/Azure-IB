
#from __future__ import print_function
import datetime as dt
import time
import pickle
import os


from ib.ext.Contract import Contract
from ib.ext.Order import Order
#from ib.ext.EClientSocket import
from ib.ext.EWrapper import EWrapper
from ib.opt import ibConnection, message as ib_message_type
from ib.opt import Connection

class ExecutionHandler(object):
    """
    Handles order execution via the Interactive Brokers API
    """

    def __init__(
        self, ib_conn, currency="USD"
    ):
        # initialize
        self.ib_conn = ib_conn
        # self.order_routing = order_routing
        self.currency = currency
        self.valid_id = None
        self.position = None
        self.contract = self.create_contract("CL",'FUT', 'NYMEX', '201606','USD')
        self.is_trading = False
        #will need  a test for pickle existence TODO panda the pickle or something
        self.fill_dict = {}
        # self.fill_dict[0] = {
        #     "timestamp": dt.datetime.now(),
        #     "id": 12,
        #     "direction": "NULL",
        #     "filled": True,
        #     "type": "dick"
        # }
        #probably unnecessary if not used as __main__
        self.last_trade = None
        self.last_bid = None
        self.last_ask = None
        self.last_fill = None




#this bellow needs to go in registration at HTFmodel
    # def _error_handler(self, msg):
    #     # error handling
    #     print("Server Error: %s" % msg)

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
            # print msg
            zboub = msg.order
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


#    def create_initial_order_id(self):
#         return 750


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

    def create_trailing_order(self, quantity, action, trail_threshold):
        """
        Creates the trailing order
        quantity: quantity of contracts to trade
        action: "BUY" or "SELL"
        trail_threshold: amount to trail the price by
        parent_id: order ID of the parent fill

        return: order object
        """
        order = Order()
        order.m_orderType = "TRAIL"  # "TRAIL" = Trailing Stop "TRAIL LIMIT" = Trailing stop limit
        order.m_totalQuantity = quantity
        order.m_lmtPrice = 0
        order.m_auxPrice = trail_threshold  # trailing amount
        order.m_action = action
        order.m_triggerMethod = 0  # midpoint method
        #order.m_parentId = parent_id
        print "trailing order spawned"
        return order

    def place_trade(self, action, last_bid, last_ask, qty=1):
        #
        #I have changed the input to add last_bid and last_ask, __main__ now probably broken!!!!!!
        #
        print "place trade activated"
        if not self.is_trading:
            if action == "BUY":
                price = last_bid + 0.5#to get filled
            if action == "SELL":
                price = last_ask - 0.5
            print str(self.valid_id)
            order = self.create_order('LMT',qty,action,price)
            print "created order from handler"
            print "main order id"
            print self.valid_id
            main_id = self.valid_id
            self.execute_order(self.contract,order)

            print self.fill_dict[main_id]
            self.is_trading = True
            print "requesting open orders"
            self.req_open()
            print "waiting for 5sec to get a fill"
            cnt = 50
            #trail_exists = False

            trail = None
            while cnt > 0:

                print "main id fill dict"
                print self.fill_dict[main_id]
                if self.fill_dict[main_id]["filled"]: # and not trail_exists:
                    if action == "BUY":
                        naction = "SELL"
                    if action == "SELL":
                        naction = "BUY"
                    trail = self.create_trailing_order(1,naction,0.02)
                    print "created trailing order at 2 ticks"
                    print "trail order id"
                    print self.valid_id
                    trail_id = self.valid_id
                    self.execute_order(self.contract,trail)

                    time.sleep(1)
                    #trail_exists = True
                    while True:
                        print "trail id fill dict"
                        print self.fill_dict[trail_id]
                        time.sleep(0.1)
                        if self.fill_dict[trail_id]["filled"]:
                            break
                    #trail_exists = False
                    print "trail got a fill"
                    break
                cnt -= 1
                time.sleep(0.1)

            if trail is not None:
                if not self.fill_dict[main_id]["filled"]:
                    print "killing order"
                    self.cancel_order(main_id)
                self.trading = False
            time.sleep(1)


    def create_fill_dict_entry(self, id, order):
        """
        Creates an entry in the Fill Dictionary that lists
        orderIds and provides security information. This is
        needed for the event-driven behaviour of the IB
        server message behaviour.
        """
        self.fill_dict[id] = {
            "timestamp": dt.datetime.now(),
            "order": order.m_orderType,
            "direction": order.m_action,
            "filled": False,
            "price": 0
        }
        print "created entry"

    def create_fill(self, msg):
        """
        Places a fill in the fill dictionary
        """


        # Check for fill and no duplicates
        self.fill_dict[msg.orderId]["filled"] = True
        #self.fill_dict[msg.orderId["price"] = msg.


    def execute_order(self, ib_contract, ib_order):
        """
        Execute the order through IB API
        """
        # send the order to IB
        self.create_fill_dict_entry(self.valid_id, ib_order)
        self.ib_conn.placeOrder(
            self.valid_id, ib_contract, ib_order
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
        if self.position !=0:
            if self.position > 0:
                neut = self.create_order("MKT",self.position,"SELL")
            if self.position < 0:
                neut = self.create_order("MKT", self.position, "BUY")
            self.execute_order(self.contract,neut)


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
    test.place_trade("BUY", test.last_bid,test.last_ask)
    time.sleep(5)
    for key in test.fill_dict:
        print key
    test.save_pickle()
    model_conn.disconnect()
