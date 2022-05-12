from fastapi import APIRouter, HTTPException, UploadFile, File
from typing import Optional
import os
import pandas as pd
import json

from utils import season, exeptions
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
        else :
            df = pd.read_csv(file.file._file)
    except:
        raise exeptions.not_valid_file

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
