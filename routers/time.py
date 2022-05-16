from fastapi import APIRouter, UploadFile, File
from typing import Optional
import os
import pandas as pd
import json

from utils import time, exeptions, preprocessing
import schemas.path

router = APIRouter(
    prefix='/time',
    tags=['classification']
)
# Time Classification endpoint


@router.post('/')
async def time_classification(limit: Optional[int] = None, file: UploadFile = File(...)):
    try:
        file_name, file_extension = os.path.splitext(file.filename)
        if file_extension == ".xlsx":
            df = pd.read_excel(file.file._file)
        else:
            df = pd.read_csv(file.file._file)
    except:
        raise exeptions.not_valid_file

    df = preprocessing.filter_data(df)
    # Calculate the invoice hour
    df = time.calculate_invoice_hour(df)
    # Calculate the frequency
    df = time.calculate_frequency(df)
    # Label morning purchases
    # df = time.label_morning_purchase(df)
    # # Label night purchases
    # df = time.label_night_purchase(df)
    # # Calculate the total number of purchases at the morning
    # df = time.calculate_morning_purchase(df)
    # # Calculate the total number of purchases at the night
    # df = time.calculate_night_purchase(df)
    # Create time segment
    df = time.calculate_time_segment(df)
    # Drop unnecessary column
    # df = time.drop_unnecessary_column(df)

    # Convert Dataframe to json format
    if limit == None:
        data = df.to_json(orient='records', force_ascii=False)
    else:
        data = df.head(limit).to_json(orient='records', force_ascii=False)

    data = json.loads(data)

    return data


@router.post('/count')
async def time_classification(file: UploadFile = File(...)):
    try:
        file_name, file_extension = os.path.splitext(file.filename)
        if file_extension == ".xlsx":
            df = pd.read_excel(file.file._file)
        else:
            df = pd.read_csv(file.file._file)
    except:
        raise exeptions.not_valid_file

    df = preprocessing.filter_data(df)
    # Calculate the invoice hour
    df = time.calculate_invoice_hour(df)
    # Calculate the frequency
    df = time.calculate_frequency(df)
    # Label morning purchases
    # df = time.label_morning_purchase(df)
    # # Label night purchases
    # df = time.label_night_purchase(df)
    # # Calculate the total number of purchases at the morning
    # df = time.calculate_morning_purchase(df)
    # # Calculate the total number of purchases at the night
    # df = time.calculate_night_purchase(df)
    # Create time segment
    df = time.calculate_time_segment(df)
    # Drop unnecessary column
    # df = time.drop_unnecessary_column(df)

    # Convert Dataframe to json format
    df = df.groupby(['Time_segment'], as_index=False).agg(
        {'CustomerID': 'count'}).rename(columns={'CustomerID': 'count'})

    # Convert Dataframe to json format
    data = df.to_json(orient='records', force_ascii=False)

    data = json.loads(data)

    return data


@router.post('/download/{segment}')
async def time_classification(segment: int, file: UploadFile = File(...)):
    try:
        file_name, file_extension = os.path.splitext(file.filename)
        if file_extension == ".xlsx":
            df = pd.read_excel(file.file._file)
        else:
            df = pd.read_csv(file.file._file)
    except:
        raise exeptions.not_valid_file

    df = preprocessing.filter_data(df)
    # Calculate the invoice hour
    df = time.calculate_invoice_hour(df)
    # Calculate the frequency
    df = time.calculate_frequency(df)
    # Create time segment
    df = time.calculate_time_segment(df)
    # Drop unnecessary column

    if segment == 0:
        response = preprocessing.to_stream(df, 'all_time')
    elif segment == 1:
        mask = df['Time_segment'] == 'صباح'
        df = df.loc[mask]
        response = preprocessing.to_stream(df, 'morning_time')
    elif segment == 2:
        mask = df['Time_segment'] == 'مساء'
        df = df.loc[mask]
        response = preprocessing.to_stream(df, 'night_time')
    elif segment == 3:
        mask = df['Time_segment'] == 'محايد'
        df = df.loc[mask]
        response = preprocessing.to_stream(df, 'neutral_time')
    else:
        raise exeptions.wrong_segment

    return response
