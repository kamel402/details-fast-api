from fastapi import APIRouter, UploadFile, File
from typing import Optional
import os
import pandas as pd
import json

from utils import discount, exeptions, preprocessing
import schemas.path

router = APIRouter(
    prefix='/discount',
    tags=['classification']
)
# Discount Classification endpoint


@router.post('/')
async def discount_classification(limit: Optional[int] = None, file: UploadFile = File(...)):
    try:
        file_name, file_extension = os.path.splitext(file.filename)
        if file_extension == ".xlsx":
            df = pd.read_excel(file.file._file)
        else:
            df = pd.read_csv(file.file._file)
    except:
        raise exeptions.not_valid_file

    df = preprocessing.filter_data(df)
    # Calculate the frequency
    df = discount.calculate_dicount(df)
    # Create discount segment
    df = discount.calculate_discount_segment(df)
    # Drop unnecessary column
    # df = discount.drop_unnecessary_column(df)
    # Convert Dataframe to json format
    if limit == None:
        data = df.to_json(orient='records', force_ascii=False)
    else:
        data = df.head(limit).to_json(orient='records', force_ascii=False)

    data = json.loads(data)

    return data


@router.post('/count')
async def discount_classification(file: UploadFile = File(...)):
    try:
        file_name, file_extension = os.path.splitext(file.filename)
        if file_extension == ".xlsx":
            df = pd.read_excel(file.file._file)
        else:
            df = pd.read_csv(file.file._file)
    except:
        raise exeptions.not_valid_file

    df = preprocessing.filter_data(df)
    # Calculate the frequency
    df = discount.calculate_dicount(df)
    # Create discount segment
    df = discount.calculate_discount_segment(df)
    # Drop unnecessary column
    # df = discount.drop_unnecessary_column(df)
    # Convert Dataframe to json format
    df = df.groupby(['Discount_segment'], as_index=False).agg(
        {'CustomerID': 'count'}).rename(columns={'CustomerID': 'count'})

    # Convert Dataframe to json format
    data = df.to_json(orient='records', force_ascii=False)

    data = json.loads(data)

    return data


@router.post('/download/{segment}')
async def discount_classification(segment: int, file: UploadFile = File(...)):
    try:
        file_name, file_extension = os.path.splitext(file.filename)
        if file_extension == ".xlsx":
            df = pd.read_excel(file.file._file)
        else:
            df = pd.read_csv(file.file._file)
    except:
        raise exeptions.not_valid_file

    df = preprocessing.filter_data(df)
    # Calculate the frequency
    df = discount.calculate_dicount(df)
    # Create discount segment
    df = discount.calculate_discount_segment(df)

    if segment == 0:
        response = preprocessing.to_stream(df, 'all_discount')
    elif segment == 2:
        mask = df['Discount_segment'] == 'مهتم بالعروض اشترى اكثر من مرة'
        df = df.loc[mask]
        response = preprocessing.to_stream(df, 'interest_more_than_1_discount')
    elif segment == 3:
        mask = df['Discount_segment'] == 'مهتم بالعروض اشترى مرة وحدة فقط'
        df = df.loc[mask]
        response = preprocessing.to_stream(df, 'interest_1_discount')
    elif segment == 1:
        mask = df['Discount_segment'] == 'عادي'
        df = df.loc[mask]
        response = preprocessing.to_stream(df, 'normal_discount')
    else:
        raise exeptions.wrong_segment

    return response
