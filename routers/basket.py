from fastapi import APIRouter, UploadFile, File
import pandas as pd

import schemas.path
from utils import basket, exeptions


router = APIRouter(
    prefix='/basket',
    tags=['basket']
)

# Basket Analysis endpoint


@router.post('/')
async def basket_analysis(file: UploadFile = File(...)):
    try:
        df = pd.read_excel(file.file)
    except:
        raise exeptions.not_valid_file

    # Generate transactions
    transactions = basket.generate_transactions(df)
    # Calculate frequent itemsets
    frequent_itemsets = basket.calculate_frequent_itemsets(transactions)
    # Generate association rules
    rules = basket.generate_rules(frequent_itemsets)
    # Convert Dataframe to json format
    data = rules.to_json(orient='records', force_ascii=False)
    return data