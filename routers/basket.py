from fastapi import APIRouter, HTTPException
from numpy import number
import pandas as pd

import schemas.path
from utils import basket


router = APIRouter(
    prefix='/basket',
    tags=['basket']
)

# Basket Analysis endpoint


@router.post('/')
async def basket_analysis(path: schemas.path.Path):
    try:
        df = pd.read_csv(path.path)
    except:
        raise HTTPException(
            status_code=404,
            detail='Not csv file'
        )

    # Generate transactions
    transactions = basket.generate_transactions(df)
    # Calculate frequent itemsets
    frequent_itemsets = basket.calculate_frequent_itemsets(transactions)
    # Generate association rules
    rules = basket.generate_rules(frequent_itemsets)

    # Convert Dataframe to json format
    data = rules.to_json(orient='records')
    return data
