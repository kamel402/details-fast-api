from fastapi import APIRouter, UploadFile, File
import pandas as pd

import json
import os

import schemas.path
from utils import basket, exeptions, preprocessing


router = APIRouter(
    prefix='/basket',
    tags=['basket']
)

# Basket Analysis endpoint


@router.post('/')
async def basket_analysis(file: UploadFile = File(...)):
    try:
        df = pd.read_csv('products.csv')
    except:
        try:
            file_name, file_extension = os.path.splitext(file.filename)
            if file_extension == ".xlsx":
                df = pd.read_excel(file.file._file)
            else:
                df = pd.read_csv(file.file._file)
        except:
            raise exeptions.not_valid_file

        # Preprocess data
        df = preprocessing.filter_data(df)
        # Generate products sheet
        df = basket.generate_products(df)
    # Generate transactions
    transactions = basket.generate_transactions(df)
    # Calculate frequent itemsets
    frequent_itemsets = basket.calculate_frequent_itemsets(transactions)
    # Generate association rules
    rules = basket.generate_rules(frequent_itemsets)
    # Convert Dataframe to json format
    data = rules.to_json(orient='records', force_ascii=False)

    data = json.loads(data)

    return data


@router.post('/download')
async def basket_analysis(file: UploadFile = File(...)):
    try:
        df = pd.read_csv('products.csv')
    except:
        try:
            file_name, file_extension = os.path.splitext(file.filename)
            if file_extension == ".xlsx":
                df = pd.read_excel(file.file._file)
            else:
                df = pd.read_csv(file.file._file)
        except:
            raise exeptions.not_valid_file

        # Preprocess data
        df = preprocessing.filter_data(df)
        # Generate products sheet
        df = basket.generate_products(df)
    # Generate transactions
    transactions = basket.generate_transactions(df)
    # Calculate frequent itemsets
    frequent_itemsets = basket.calculate_frequent_itemsets(transactions)
    # Generate association rules
    rules = basket.generate_rules(frequent_itemsets)

    response = preprocessing.to_stream(rules, 'basket_patterns')

    return response
