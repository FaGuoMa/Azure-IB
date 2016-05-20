# Azure-IB
A (relatively) high-frequency trading system focused on CME's WTI (Light Sweet Crude Oil) outright Future. It builds on:
- Interactive Brokers Gateway (interactivebrokers.com) and IBPY, a wrapper for API calls( https://github.com/blampe/IbPy)
- Microsoft's (fantastic) Machine Learning cloud-based solution, called through REST API. Our stack there is fairly complex but any signal generation should work
- concurrent.Futures for, well, concurrency
- plot.ly for stream strategy execution to a remote charting display

It has been tested on a headless AWS server running Ubuntu. This is a very crude approach and we will not be maintaining it
