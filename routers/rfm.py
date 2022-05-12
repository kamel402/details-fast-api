from fastapi import APIRouter, UploadFile, File
from typing import Optional
import pandas as pd
import json
import os

from utils import rfm, exeptions, preprocessing
import schemas.path

router = APIRouter(
    prefix='/rfm',
    tags=['classification']
)
# RFM Classification endpoint


@router.post('/')
async def rfm_classification(limit: Optional[int] = None, file: UploadFile = File(...)):
    try:
        file_name, file_extension = os.path.splitext(file.filename)
        if file_extension == ".xlsx":
            df = pd.read_excel(file.file._file)
        else :
            df = pd.read_csv(file.file._file)
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
    # df = rfm.calculate_city(df)
    # # Unite payment methods
    # df = rfm.calculate_payment_method(df)
    # Drop unnecessary columns
    # df = rfm.drop_unnecessary_columns(df)
    # Convert Dataframe to json format
    if limit == None:
        data = df.to_json(orient='records', force_ascii=False)
    else:
        data = df.head(limit).to_json(orient='records', force_ascii=False)

    data = json.loads(data)

    return data

@router.post('/count')
async def rfm_classification(file: UploadFile = File(...)):
    try:
        file_name, file_extension = os.path.splitext(file.filename)
        if file_extension == ".xlsx":
            df = pd.read_excel(file.file._file)
        else :
            df = pd.read_csv(file.file._file)
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
    # # Unite cities
    # df = rfm.calculate_city(df)
    # # Unite payment methods
    # df = rfm.calculate_payment_method(df)
    # Drop unnecessary columns
    df = rfm.drop_unnecessary_columns(df)

    df = df.groupby(['general_segment'],as_index=False).agg({'CustomerID': 'count'}).rename(columns = {'CustomerID': 'count'})

    # Convert Dataframe to json format
    data = df.to_json(orient='records', force_ascii=False)
    
    data = json.loads(data)

    return data

