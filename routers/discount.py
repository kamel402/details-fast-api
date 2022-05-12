from fastapi import APIRouter, UploadFile, File
from typing import Optional
import os
import pandas as pd
import json

from utils import discount, exeptions
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
        else :
            df = pd.read_csv(file.file._file)
    except:
        raise exeptions.not_valid_file

    # Calculate the frequency
    df = discount.calculate_frequency(df)
    # Gets transaction with discounts only
    df = discount.is_discount(df)
    # Calculate number of discounts
    df = discount.calculate_number_of_discounts(df)
    # Calculate amount of discount
    df = discount.calculate_amount_of_discount(df)
    # Calculate the amount
    df = discount.calculate_amount(df)
    # Calculate discounts percentege
    df = discount.calculate_discounts_percentege(df)
    # Create discount segment
    df = discount.calculate_discount_segment(df)
    # Drop unnecessary column
    df = discount.drop_unnecessary_column(df)
    # Convert Dataframe to json format
    if limit == None:
        data = df.to_json(orient='records', force_ascii=False)
    else:
        data = df.head(limit).to_json(orient='records', force_ascii=False)

    data = json.loads(data)

    return data
