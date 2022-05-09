from fastapi import APIRouter, UploadFile, File
import pandas as pd

from utils import time, exeptions
import schemas.path

router = APIRouter(
    prefix='/time',
    tags=['classification']
)
# Time Classification endpoint


@router.post('/')
async def time_classification(file: UploadFile = File(...)):
    try:
        df = pd.read_excel(file.file._file)
    except:
        raise exeptions.not_valid_file

    # Calculate the invoice hour
    df = time.calculate_invoice_hour(df)
    # Calculate the frequency
    df = time.calculate_frequency(df)
    # Label morning purchases
    df = time.label_morning_purchase(df)
    # Label night purchases
    df = time.label_night_purchase(df)
    # Calculate the total number of purchases at the morning
    df = time.calculate_morning_purchase(df)
    # Calculate the total number of purchases at the night
    df = time.calculate_night_purchase(df)
    # Create time segment
    df = time.calculate_time_segment(df)
    # Drop unnecessary column
    df = time.drop_unnecessary_column(df)

    # Convert Dataframe to json format
    data = df.to_json(orient='records', force_ascii=False)
    print(data)
    return data