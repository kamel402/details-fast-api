# 1. Library imports
import uvicorn
from fastapi import FastAPI, HTTPException, UploadFile, File
import numpy as np
import pandas as pd
import math
from sklearn import preprocessing
from utils import rfm
# 2. Create the app object
app = FastAPI()

# 3. Index route, opens automatically on http://127.0.0.1:8000
@app.get('/')
async def index():
    return {'message': 'Wellcom'}

# 3. Expose the prediction functionality, make a prediction from the passed
#    JSON data and return the predicted customer segment
@app.post('/rfm')
async def rfm_classification(file:UploadFile = File(...)):
    try:
        df = pd.read_csv(file.file)
    except:
        raise HTTPException(
            status_code= 404,
            detail= 'Not csv file'
        )    
    
    # Calculating the rfm
    df = rfm.calculate_rfm(df)
    # Normlizing the rfm
    df = rfm.calculate_rfm_logs(df)
    # Divide rfm into 4 categories
    df = rfm.calculate_4_categories(df)
    # Create quartiles named RFM_segment
    df = rfm.rfm_segment(df)
    # Calculate rfm score by summing up the RFM quartile metrics
    df = rfm.rfm_score(df)
    # Create a general segment
    df = rfm.calculate_general_segment(df)
    # Unite cities
    df = rfm.calculate_city(df)
    # Unite payment methods
    df = rfm.calculate_payment_method(df)
    # Convert Dataframe to json format 
    data = df.to_json(orient='records')

    return data
    

# 5. Run the API with uvicorn
#    Will run on http://127.0.0.1:8000
if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8000)
    
#uvicorn app:app --reload