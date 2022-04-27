from fastapi import APIRouter, HTTPException
import pandas as pd

from utils import rfm
import schemas.path

router = APIRouter(
    prefix='/rfm',
    tags=['classification']
)
# RFM Classification endpoint


@router.post('/')
async def rfm_classification(path: schemas.path.Path):
    try:
        df = pd.read_csv(path.path)
    except:
        raise HTTPException(
            status_code=404,
            detail='Not csv file'
        )

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
    # Convert Dataframe to json format
    data = df.to_json(orient='records', force_ascii=False)

    return data
