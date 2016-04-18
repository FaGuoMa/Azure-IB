"""
Z score trading algorithm logic

updated: 2016-04-17

Author: Derek Wong
"""
# update v0.1 added trend logic. simulated flags in dataframe. accepted flags for algo logic switching
# update v0.2 refactored into a class and takes bbo. use decimal package to prevent float errors

import decimal
# import execution_handler

FIVEPLACES = decimal.Decimal("0.00001")

class Zscore:
    """
    Zscore class function
    initialize takes:
        init_bid = initial bid value
        init_ask = initial ask value
        init_zscore = initial zscore
        init_mean = initial mean value
        init_state = initial state ("FLAT", "LONG", "SHORT")
        init_flag = initial flag ("trend", "range")
    """
    def __init__(self, init_bid, init_ask, init_zscore, init_mean, init_stdev, init_state, init_flag):
        self.bid = decimal.Decimal(init_bid).quantize(FIVEPLACES)
        self.ask = decimal.Decimal(init_ask).quantize(FIVEPLACES)
        self.zscore = decimal.Decimal(init_zscore).quantize(FIVEPLACES)
        self.mean = decimal.Decimal(init_mean).quantize(FIVEPLACES)
        self.stdev = decimal.Decimal(init_stdev).quantize(FIVEPLACES)
        self.state = init_state
        self.flag = init_flag
        self.mid_price = self.calc_mid_price(init_bid, init_ask)
        # initialize signals to NONE
        self.signal = "NONE"

        # initialize historical values same as current values at initialization
        self.hist_bid = self.bid
        self.hist_ask = self.ask
        self.hist_zscore = self.zscore
        self.hist_mean = self.mean
        self.hist_flag = self.flag
        self.hist_stdev = self.stdev

        # initialize internal variables
        self.state = "FLAT"
        self.signal = "NONE"

        self.hist_state = self.state
        self.hist_signal = self.signal

        # initialize order handler
#        self.execution = execution_handler.ExecutionHandler()

#    def init_execution_handler(self, symbol, sec_type, exch, prim_exch, curr):
#        # initialize the execution handler for the given contract
#
#        # set contract parameters
#        self.contract = self.execution.create_contract(symbol, sec_type, exch, prim_exch, curr)
#
#        # create buy and sell orders
#        self.buy_order = self.execution.create_order(order_type = "MKT", quantity=1, action="BUY")
#        self.sell_order = self.execution.create_order(order_type = "MKT", quantity=1, action="SELL")


    def set_parameters(self, n_sma=30, n_stdev=30, z_threshold=2, z_close_thresh=0.2):
        # parameter initializations if we decide to calculate internally
        self.n_sma = n_sma
        self.n_stdev = n_stdev
        self.z_threshold = z_threshold
        self.z_close_thresh = z_close_thresh

    def calc_mid_price(self, bid, ask):
        # update the mid price by using average of bid/ask
        mid_price = decimal.Decimal((bid + ask) / 2).quantize(FIVEPLACES)
        self.mid_price = mid_price
        return mid_price

    def update_bbo(self, new_bid, new_ask):
        # update the bbo and mid price given the new values. store previous values
        self.hist_bid = self.bid
        self.hist_ask = self.ask

        self.bid = decimal.Decimal(new_bid).quantize(FIVEPLACES)
        self.ask = decimal.Decimal(new_ask).quantize(FIVEPLACES)

        self.mid_price = self.calc_mid_price(bid=new_bid, ask=new_ask)

    def update_mean_stdev(self, new_mean, new_stdev):
        # update the mean & stdev possibly from the 1 minute data. store previous values

        self.hist_mean = self.mean
        self.mean = new_mean

        self.hist_stdev = self.stdev
        self.stdev = new_stdev

    def update_flag(self, new_flag):
        # update the flag
        self.hist_flag = self.flag
        self.flag = new_flag

    def calc_zscore(self, mid_price):
        new_zscore = (mid_price - decimal.Decimal(self.mean)) / decimal.Decimal(self.stdev)
        self.zscore = new_zscore

    def algo_calc(self):
        # Enter Long Position Signal in trend flag
        if self.zscore > self.z_threshold and \
                self.hist_zscore <= self.z_threshold and \
                self.flag == "trend" and \
                self.state == "FLAT":

            self.hist_state = self.state
            self.hist_signal = self.signal

            self.signal = "BOT"
            self.state = "LONG"
            model.ib_utils.create_stock_order(1, True, True)
            print "buy order sent"

        # Enter Short Position Signal in trend flag
        elif self.zscore < -self.z_threshold and \
                self.zscore >= -self.z_threshold and \
                self.flag == "trend" and \
                self.state == "FLAT":

            self.hist_state = self.state
            self.hist_signal = self.signal

            self.signal = "SLD"
            self.state = "SHORT"
            model.ib_utils.create_stock_order(1, False, True)
            print "sell order sent"

        # Close Long position in trend flag
        elif self.hist_state == "LONG" and \
                self.zscore <= self.z_close_thresh and \
                self.hist_state == "trend":

            self.hist_state = self.state
            self.hist_signal = self.signal

            self.signal = "SLD"
            self.state = "FLAT"
            model.ib_utils.create_stock_order(1, False, True)
            print "sell order sent"

        # Close Short Position in trend flag
        elif self.hist_state == "SHORT" and \
                self.zscore >= -self.z_close_thresh and\
                self.flag == "trend":

            self.hist_state = self.state
            self.hist_signal = self.signal

            self.signal = "BOT"
            self.state = "FLAT"
            model.ib_utils.create_stock_order(1, True, True)
            print "buy order sent"

        # Enter Long Position Signal in range flag
        elif self.zscore < -self.z_threshold and \
                self.hist_zscore >= -self.z_threshold and \
                self.flag == "range" and \
                self.hist_state == "FLAT":

                self.hist_state = self.state
                self.hist_signal = self.signal

                self.signal = "BOT"
                self.state = "LONG"
                model.ib_utils.create_stock_order(1, True, True)
                print "buy order sent"

        # Enter Short Position Signal in range flag
        elif self.zscore > self.z_threshold and \
                self.hist_zscore <= self.z_threshold and \
                self.flag == "range" and \
                self.hist_state == "FLAT":

                self.hist_state = self.state
                self.hist_signal = self.signal

                self.signal = "SLD"
                self.state = "SHORT"
                model.ib_utils.create_stock_order(1, False, True)
                print "sell order sent"

        # Close Long position in range flag
        elif self.hist_state == "LONG" and \
                self.zscore >= -self.z_close_thresh and \
                self.flag == "range":

            self.hist_state = self.state
            self.hist_signal = self.signal

            self.signal = "SLD"
            self.state = "FLAT"
            model.ib_utils.create_stock_order(1, False, True)
            print "sell order sent"

        # Close Short Position in range flag
        elif self.hist_state == "SHORT" and \
                        self.zscore <= self.z_close_thresh and \
                        self.flag == "range":

            self.hist_state = self.state
            self.hist_signal = self.signal

            self.signal = "BOT"
            self.state = "FLAT"

            model.ib_utils.create_stock_order(1, True, True)
            print "buy order sent"

    def stops_calc(self):
        # calculate any stop trade conditions
        if self.state == "FLAT":
            pass

        #stop distance is 1/2 of the zscore trigger threshold
        stop_dist = self.z_threshold/2

        # Long Trend Stop
        if self.state == "LONG" and self.flag == "trend" \
                and stop_dist >= self.zscore:

            self.hist_state = self.state
            self.hist_signal = self.signal

            self.signal = "SLD"
            self.state = "FLAT"
            model.ib_utils.create_stock_order(1, False, True)
            print "sell order sent"

        # Short Trend Stop
        if self.state == "short" and self.flag == "trend" \
                and -stop_dist <= self.zscore:

            self.hist_state = self.state
            self.hist_signal = self.signal

            self.signal = "BOT"
            self.state = "FLAT"
            model.ib_utils.create_stock_order(1, True, True)
            print "buy order sent"

        # Long range Stop
        if self.state == "LONG" and self.flag == "range" \
                and stop_dist+self.z_threshold <= self.zscore:
            self.hist_state = self.state
            self.hist_signal = self.signal

            self.signal = "SLD"
            self.state = "FLAT"
            model.ib_utils.create_stock_order(1, False, True)
            print "sell order sent"

        # Short range Stop
        if self.state == "short" and self.flag == "trend" \
                and -stop_dist-self.z_threshold >= self.zscore:
            self.hist_state = self.state
            self.hist_signal = self.signal

            self.signal = "BOT"
            self.state = "FLAT"
            model.ib_utils.create_stock_order(1, True, True)
            print "buy order sent"

    def print_status(self):
        # print states and status
        print "State:{state} Signal:{signal} Flag: {flag} Mid:{mid} Zscore:{zscore}".format(state=self.state,
                                                                                            signal=self.signal,
                                                                                            flag=self.flag,
                                                                                            mid=self.mid_price,
                                                                                            zscore=self.zscore)

    def on_tick(self, cur_bid, cur_ask):
        # every tick pass the bid ask, perform calcs

        # take current bid ask and calculate mid price
        self.calc_mid_price(bid=decimal.Decimal(cur_bid), ask=decimal.Decimal(cur_ask))

        # calculate the new z score for the given tick mid price
        self.calc_zscore(self.mid_price)

        # calculate stops
        self.stops_calc()

        # run algo calc based on object self values
        self.algo_calc()

        #print status
        self.print_status()

    def on_minute(self, new_mean, new_stdev, new_flag):
        # update the parameters every minute

        self.update_mean_stdev(new_mean=new_mean, new_stdev=new_stdev)
        self.update_flag(new_flag=new_flag)
        self.print_status()





