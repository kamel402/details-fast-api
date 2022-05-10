from fastapi import APIRouter, UploadFile, File
import pandas as pd
import json

from utils import rfm, exeptions, preprocessing
import schemas.path

router = APIRouter(
    prefix='/rfm',
    tags=['classification']
)
# RFM Classification endpoint


@router.post('/')
async def rfm_classification(file: UploadFile = File(...)):
    try:
        df = pd.read_excel(file.file._file)
    except:
        raise exeptions.not_valid_file

    df = preprocessing.filter_data(df)
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
    # Drop unnecessary columns
    df = rfm.drop_unnecessary_columns(df)
    # Convert Dataframe to json format
    data = df.to_json(orient='records', force_ascii=False)

    data = json.loads(data)

    return data

