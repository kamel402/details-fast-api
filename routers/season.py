from fastapi import APIRouter, HTTPException, UploadFile, File
from typing import Optional
import os
import pandas as pd
import json

from utils import season, exeptions, preprocessing
import schemas.path

router = APIRouter(
    prefix='/season',
    tags=['classification']
)
# Season Classification endpoint


@router.post('/')
async def season_classification(limit: Optional[int] = None, file: UploadFile = File(...)):
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
    df = season.calculate_frequency(df)
    # Convert Gregorian date to Hijri date
    df = season.convert_date_to_hijri(df)
    # Calculate perchuse only in Eid Al-Fitr
    df = season.calculate_only_fitr_purchase(df)
    # Calculate perchuse only in Eid Al-Adha
    df = season.calculate_only_adha_purchase(df)
    # Calculate perchuse only in Eid al-Adha and al-Fitr
    df = season.calculate_both_eid_purchase(df)
    # Calculate perchuse in all season
    df = season.calculate_all_season_purchase(df)
    # Rename features
    df = season.rename_features(df)
    # Calculate season segment
    df = season.calculate_season_segment(df)

    # Convert Dataframe to json format
    if limit == None:
        data = df.to_json(orient='records', force_ascii=False)
    else:
        data = df.head(limit).to_json(orient='records', force_ascii=False)

    data = json.loads(data)

    return data


@router.post('/count')
async def season_classification(file: UploadFile = File(...)):
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
    df = season.calculate_frequency(df)
    # Convert Gregorian date to Hijri date
    df = season.convert_date_to_hijri(df)
    # Calculate perchuse only in Eid Al-Fitr
    df = season.calculate_only_fitr_purchase(df)
    # Calculate perchuse only in Eid Al-Adha
    df = season.calculate_only_adha_purchase(df)
    # Calculate perchuse only in Eid al-Adha and al-Fitr
    df = season.calculate_both_eid_purchase(df)
    # Calculate perchuse in all season
    df = season.calculate_all_season_purchase(df)
    # Rename features
    df = season.rename_features(df)
    # Calculate season segment
    df = season.calculate_season_segment(df)

    # Convert Dataframe to json format
    df = df.groupby(['season_segment'], as_index=False).agg(
        {'CustomerID': 'count'}).rename(columns={'CustomerID': 'count'})

    # Convert Dataframe to json format
    data = df.to_json(orient='records', force_ascii=False)

    data = json.loads(data)

    return data


@router.post('/download/{segment}')
async def season_classification(segment: int, file: UploadFile = File(...)):
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
    df = season.calculate_frequency(df)
    # Convert Gregorian date to Hijri date
    df = season.convert_date_to_hijri(df)
    # Calculate perchuse only in Eid Al-Fitr
    df = season.calculate_only_fitr_purchase(df)
    # Calculate perchuse only in Eid Al-Adha
    df = season.calculate_only_adha_purchase(df)
    # Calculate perchuse only in Eid al-Adha and al-Fitr
    df = season.calculate_both_eid_purchase(df)
    # Calculate perchuse in all season
    df = season.calculate_all_season_purchase(df)
    # Rename features
    df = season.rename_features(df)
    # Calculate season segment
    df = season.calculate_season_segment(df)

    if segment == 0:
        response = preprocessing.to_stream(df, 'all_season')
    elif segment == 1:
        mask = df['season_segment'] == "طوال العام"
        df = df.loc[mask]
        response = preprocessing.to_stream(df, 'all_year_season')
    elif segment == 2:
        mask = df['season_segment'] == "عيد الاضحى"
        df = df.loc[mask]
        response = preprocessing.to_stream(df, 'adha_eid_season')
    elif segment == 3:
        mask = df['season_segment'] == "عيد الفطر"
        df = df.loc[mask]
        response = preprocessing.to_stream(df, 'fitr_eid_season')
    else:
        raise exeptions.wrong_segment

    return response
