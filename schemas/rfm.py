from pydantic import BaseModel
from typing import Optional, List

# the schems are pydantic models calsses used to garantee and validate that the fields sent from the user are the field that i need.
# also it is used to make the response model which is the sample of the response.


class RFMresponse(BaseModel):
    pass
