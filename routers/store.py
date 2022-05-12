from fastapi import APIRouter, UploadFile, File
import pandas as pd
import numpy as np
import os

from utils import exeptions, preprocessing

router = APIRouter(
    prefix='/store',
    tags=['store level']
)

# favorite payment method endpoint


@router.post('/favorite_payment')
async def favorite_payment(file: UploadFile = File(...)):
    try:
        file_name, file_extension = os.path.splitext(file.filename)
        if file_extension == ".xlsx":
            df = pd.read_excel(file.file._file)
        else :
            df = pd.read_csv(file.file._file)
    except:
        raise exeptions.not_valid_file

    df = preprocessing.filter_data(df)

    fav_payment = df['payment_method'].value_counts() / df.shape[0] * 100

    fav_payment = np.array([fav_payment.index, fav_payment.astype(float)])
    fav_payment = np.transpose(fav_payment, axes=None)

    return {'favorite_payment': fav_payment.tolist()}

# store daily_orders endpoint


@router.post('/daily_orders')
async def daily_orders(file: UploadFile = File(...)):
    try:
        file_name, file_extension = os.path.splitext(file.filename)
        if file_extension == ".xlsx":
            df = pd.read_excel(file.file._file)
        else :
            df = pd.read_csv(file.file._file)
    except:
        raise exeptions.not_valid_file

    df = preprocessing.filter_data(df)

    daily_orders = df[['InvoiceDate', 'amount']]
    daily_orders['InvoiceDate'] = daily_orders['InvoiceDate'].dt.date

    daily_orders = daily_orders[['InvoiceDate', 'amount']].replace(
        0, np.nan, inplace=False)
    daily_orders = daily_orders.groupby(['InvoiceDate']).count().reset_index()
    daily_orders.columns = ['InvoiceDate', 'orders']

    dfs = pd.DataFrame(pd.DataFrame(pd.date_range(start=daily_orders['InvoiceDate'].min(
    ), end=daily_orders['InvoiceDate'].max()))[0].apply(lambda x: x.date()))
    dfs['orders'] = 0.0
    dfs.columns = ['InvoiceDate', 'orders']

    daily_orders = pd.concat([dfs, daily_orders], axis=0).sort_values(
        ['InvoiceDate'], ascending=True)

    daily_orders = daily_orders.groupby('InvoiceDate').sum().reset_index()

    daily_orders['InvoiceDate'] = pd.to_datetime(daily_orders['InvoiceDate'])

    daily_orders['InvoiceDate'] = daily_orders.InvoiceDate.values.astype(
        np.int64) // 10 ** 6

    return {'daily_orders': daily_orders.to_numpy().tolist()}

# store daily_sales endpoint


@router.post('/daily_sales')
async def daily_sales(file: UploadFile = File(...)):
    try:
        file_name, file_extension = os.path.splitext(file.filename)
        if file_extension == ".xlsx":
            df = pd.read_excel(file.file._file)
        else :
            df = pd.read_csv(file.file._file)
    except:
        raise exeptions.not_valid_file

    df = preprocessing.filter_data(df)

    daily_sales = df[['InvoiceDate', 'amount']]
    daily_sales['InvoiceDate'] = daily_sales['InvoiceDate'].dt.date

    dfs = pd.DataFrame(pd.DataFrame(pd.date_range(start=daily_sales['InvoiceDate'].min(
    ), end=daily_sales['InvoiceDate'].max()))[0].apply(lambda x: x.date()))
    dfs['amount'] = 0.0
    dfs.columns = ['InvoiceDate', 'amount']

    daily_sales = pd.concat([dfs, daily_sales], axis=0).sort_values(
        ['InvoiceDate'], ascending=True)

    daily_sales = daily_sales.groupby('InvoiceDate').sum().reset_index()

    daily_sales['InvoiceDate'] = pd.to_datetime(daily_sales['InvoiceDate'])

    daily_sales['InvoiceDate'] = daily_sales.InvoiceDate.values.astype(
        np.int64) // 10 ** 6

    return {'daily_sales': [daily_sales.to_numpy().tolist()]}



@router.post('/top_cities')
async def top_cities(file: UploadFile = File(...)):
    try:
        file_name, file_extension = os.path.splitext(file.filename)
        if file_extension == ".xlsx":
            df = pd.read_excel(file.file._file)
        else :
            df = pd.read_csv(file.file._file)
    except:
        raise exeptions.not_valid_file

    df = preprocessing.filter_data(df)

    return