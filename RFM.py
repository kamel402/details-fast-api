from pydantic import BaseModel
# 2. Class which describes RFM measurements
class RFM(BaseModel):
    recency: float 
    frequency: float 
    monetary: float 