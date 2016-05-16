# Azure-IB
A (relatively) high-frequency trading system focused on CME's WTI outright Future. It build on:
- Interactive Brokers Gateway (interactivebrokers.com) and IBPY, a wrapper for API calls( https://github.com/blampe/IbPy)
- Microsoft's cloud-based solution, called through REST API
- concurrent.Futures for, well, concurrency
- plot.ly for stream strategy execution to a remote charting display

It has been tested on a headless AWS server running Ubuntu
