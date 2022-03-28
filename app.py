# 1. Library imports
import uvicorn
from fastapi import FastAPI, HTTPException
from RFM import RFM
import numpy as np
import pickle
import pandas as pd
import math
from sklearn import preprocessing

# 2. Create the app object
app = FastAPI()
pickle_in = open("rfm_classifier.pkl","rb")
rfm_classifier=pickle.load(pickle_in)

# 3. Index route, opens automatically on http://127.0.0.1:8000
@app.get('/')
async def index():
    return {'message': 'Wellcom'}

# 3. Expose the prediction functionality, make a prediction from the passed
#    JSON data and return the predicted customer segment
@app.post('/predict')
async def predict_rfm_customer_segment(data:RFM):
    data = data.dict()
    recency=data['recency']
    frequency=data['frequency']
    monetary=data['monetary']
    try:
        recency_log = math.log(recency)
        frequency_log = math.log(frequency)
        monetary_log = math.log(monetary)
    except:
        raise HTTPException(
            status_code= 404,
            detail= 'log 0 is undefined'
        )

    feature_vector = [monetary, recency, frequency]
    X_subset = np.array(feature_vector)
    X_subset = X_subset.reshape(1, -1)
    scaler = preprocessing.StandardScaler().fit(X_subset)
    X = scaler.transform(X_subset)

    prediction = rfm_classifier.predict(X)
    if(prediction[0] == 0):
        prediction="This Customer is a Potential Loyal"
    elif(prediction[0] == 1):
        prediction="This Customer is a New Customer"
    elif(prediction[0] == 2):
        prediction="This Customer is a Loyal Customer"
    else:
        prediction="This Customer is At-Risk"
    return {
        'prediction': prediction
    }

# 5. Run the API with uvicorn
#    Will run on http://127.0.0.1:8000
if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8000)
    
#uvicorn app:app --reload