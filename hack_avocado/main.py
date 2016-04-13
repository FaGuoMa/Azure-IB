"""

"""
from models.hft_model import HFTModel
import time

if __name__ == "__main__":
    model = HFTModel(host='localhost',
                     port=4001,
                     client_id=101,
                     is_use_gateway=False,
                     evaluation_time_secs=20,
                     resample_interval_secs='30s')
    model.start("CL", 100)
    time.sleep(4500)
    model.conn.disconnect()