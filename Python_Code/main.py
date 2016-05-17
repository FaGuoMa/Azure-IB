
#from models.hft_model import HFTModel
import models.hft_model as _hftmodel
import time

model = _hftmodel.HFTModel(host='localhost',
                 port=4001,
                 client_id=101,
                 is_use_gateway=False,test=False)
model.start("CL")
