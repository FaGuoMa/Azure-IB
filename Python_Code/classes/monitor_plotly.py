# -*- coding: utf-8 -*-
"""
Created on Thu Apr 14 20:35:47 2016
plotly monitor
@author: maxime_back
"""

# (*) To communicate with Plotly's server, sign in with credentials file
import plotly.plotly as py

# (*) Useful Python/Plotly tools
import plotly.tools as tls

# (*) Graph objects to piece together plots
from plotly.graph_objs import *
import datetime as dt
from params import settings
class Monit_stream:

    def __init__(self):
        #authenticate using settings
        tls.set_credentials_file(username=settings.PLOTLY_USER, api_key=settings.PLOTLY_API)
        tls.set_credentials_file(stream_ids=["wgqspsraap",
                                             "x2ud202z0t",
                                             "j7yjtjcxu7",
                                             "cgj0kteviv",
                                             "p8l5y19psu"])
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
            stream=Stream(token=self.credentials[0])  # (!) link stream id to token
            )

# Set limits and mean, but later
        self.limit_up = Scatter(
            x=[],  # init. data lists
            y=[],
            mode='lines',                             # path drawn as line
            line=Line(color='rgba(31,119,180,0.15)'), # light blue line color
            stream=Stream(
            token=self.credentials[1]         # plot a max of 100 pts on screen
            )
            )
        self.limit_dwn = Scatter(
            x=[],  # init. data lists
            y=[],
            mode='lines',                             # path drawn as line
            line=Line(color='rgba(31,119,180,0.15)'), # light blue line color
            stream=Stream(
            token=self.credentials[2]# plot a max of 100 pts on screen
            )
            )
        self.ranging = Scatter(
            x=[],  # init. data lists
            y=[],
            mode='markers',
            line=Line(color='rgba(200,0,0,0.5)'), # red if the system thinks it ranges
              # reduce opacity
            marker=Marker(size=5),  # increase marker size
            stream=Stream(token=self.credentials[3])
            )

        self.fills = Scatter(
            x=[],  # init. data lists
            y=[],
            mode='markers',

            marker=Marker(size=15, color='rgba(191,63,191,0.7)'),  # increase marker size
            stream=Stream(token=self.credentials[4])
        )
# (@) Send fig to Plotly, initialize streaming plot, open tab
        self.stream1 = py.Stream(self.credentials[0])

# (@) Make 2nd instance of the stream link object, 
#     with same stream id as the 2nd stream id object (in trace2)
        self.stream2 = py.Stream(self.credentials[1])
        self.stream3 = py.Stream(self.credentials[2])
        self.stream4 = py.Stream(self.credentials[3])
        self.stream5 = py.Stream(self.credentials[4])
# data
        self.data = Data([self.prices,self.limit_up,self.limit_dwn,self.ranging, self.fills])
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
        print "streams initaited"
        
    def update_data_point(self,last_price,last_mean,last_sd,flag):
        now = dt.datetime.now()        
        self.stream1.write(dict(x=now, y=last_price))
        self.stream2.write(dict(x=now, y=last_mean+settings.Z_THRESH*last_sd))
        self.stream3.write(dict(x=now, y=last_mean-settings.Z_THRESH*last_sd))

        if flag == "range":
            self.stream4.write(dict(x=now, y=last_price))

    def update_fills(self, fill):
        #now=dt.datetime.now()
        if fill is not None:
            self.stream5.write(dict(x=fill[0], y=fill[1]))

    def close_stream(self):
        self.stream1.close()
        self.stream2.close()
        self.stream3.close()
        self.stream4.close()
        self.stream5.close()
        # (@) Write 1 point corresponding to 1 pt of path,
        #     appending the data on the plot

                                 