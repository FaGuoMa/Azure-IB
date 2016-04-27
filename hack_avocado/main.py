"""

"""
import datetime as dt
import pytz
from models.hft_model import HFTModel
import time
from apscheduler.schedulers.blocking import BlockingScheduler

def next_market_open():
    now = dt.datetime.now()
    tz_cme = pytz.timezone('America/Chicago')
    cme_now = pytz.timezone('Singapore').localize(now).astimezone(tz_cme)
    if cme_now.weekday() == 4 or cme_now.weekday() ==5:
        next_open = cme_now.replace(hour=17,minute=0,second=0)
        next_open = next_open + dt.timedelta(days=6-cme_now.weekday())
    elif cme_now.hour < 17:
        next_open = cme_now.replace(hour=17, minute=0, second=0)       
    elif cme_now.hour > 17:
        next_open = cme_now.replace(hour=17, minute=0, second=0)
        next_open = next_open + dt.timedelta(days=1)            
    next_open = next_open.astimezone(pytz.timezone('Singapore'))
    return next_open

def next_market_close():
    now = dt.datetime.now()
    tz_cme = pytz.timezone('America/Chicago')
    cme_now = pytz.timezone('Singapore').localize(now).astimezone(tz_cme)
    if cme_now.weekday() == 5 or cme_now.weekday() ==6:
        next_close = cme_now.replace(hour=16,minute=0,second=0)
        next_close = next_close + dt.timedelta(days=7-cme_now.weekday())
    elif cme_now.hour < 16:
        next_close = cme_now.replace(hour=16, minute=0, second=0)       
    elif cme_now.hour > 16:
        next_close = cme_now.replace(hour=16, minute=0, second=0)
        next_close = next_close + dt.timedelta(days=1)            
    next_close = next_close.astimezone(pytz.timezone('Singapore'))
    return next_close

def spawn_model():
    model = HFTModel(host='localhost',
                     port=4001,
                     client_id=101,
                     is_use_gateway=False)
    model.start("CL", 100)

#very, very dirty
def stop_and_go():
    model.stop()
    scheduler.shutdown()
    scheduler.add_job(spawn_model, 'date', run_date=next_market_open())
    scheduler.add_job(stop_and_go, 'date', run_date=next_market_close())
    scheduler.start()
    
    

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    # scheduler.add_job(spawn_model, 'date', run_date=next_market_open())
    spawn_model()
    # scheduler.add_job(stop_and_go, 'date', run_date=next_market_close())
    # scheduler.start()

#to keep in mind    
    #scheduler.shutdown()