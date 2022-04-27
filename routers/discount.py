from fastapi import APIRouter, HTTPException
import pandas as pd

from utils import discount
import schemas.path

router = APIRouter(
    prefix='/discount',
    tags=['classification']
)
# Discount Classification endpoint


@router.post('/')
async def discount_classification(path: schemas.path.Path):
    try:
        df = pd.read_csv(path.path)
    except:
        raise HTTPException(
            status_code=404,
            detail='Not csv file'
        )

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
    data = df.to_json(orient='records', force_ascii=False)
    return data
