from fastapi import APIRouter, UploadFile, File
import pandas as pd
import numpy as np
import datetime
import os

from utils import exeptions, preprocessing, basket

router = APIRouter(
    prefix='/store',
    tags=['store level']
)


@router.post('/overview')
async def overview(file: UploadFile = File(...)):
    try:
        file_name, file_extension = os.path.splitext(file.filename)
        if file_extension == ".xlsx":
            df = pd.read_excel(file.file._file)
        else :
            df = pd.read_csv(file.file._file)
    except:
        raise exeptions.not_valid_file

    df = preprocessing.filter_data(df)

    # No. Customers
    num_customers = len(df['CustomerID'].unique())

    # No. Months
    end_date = df.InvoiceDate.min()
    start_date = datetime.datetime.now()
    num_months = (start_date.year - end_date.year) * 12 + (start_date.month - end_date.month)

    # Total sales
    total_sales = int(df['amount'].sum())

    # No. successful transactions
    successful_transactions = len(df['InvoiceNo'])

    return {'store_customer': num_customers, 'months_since_founded': num_months, 'total_sales': total_sales, 'successful_transactions':successful_transactions}


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

    fav_payment = df['payment_method'].value_counts()

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

    return {'daily_sales': daily_sales.to_numpy().tolist()}



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
    cities = df['city'].value_counts()
    cities = df['city'].value_counts().to_dict()
    cities_list = list(cities.keys())
    cities_list = cities_list[0:10]
    numbers = list(cities.values())
    numbers = numbers[0:10]
    return {'cities': cities_list, 'numbers': numbers}


@router.post('/top_products')
async def top_10_products(file: UploadFile = File(...)):
    try:
        df = pd.read_csv('products.csv')
    except:
        try:
            file_name, file_extension = os.path.splitext(file.filename)
            if file_extension == ".xlsx":
                df = pd.read_excel(file.file._file)
            else :
                df = pd.read_csv(file.file._file)
        except:
            raise exeptions.not_valid_file
    
        # Preprocess data
        df = preprocessing.filter_data(df)
        # Generate products sheet
        df = basket.generate_products(df)
    
    

    products = df['products'].value_counts()
    products = df['products'].value_counts().to_dict()
    products_list = list(products.keys())
    products_list = products_list[0:10]
    numbers = list(products.values())
    numbers = numbers[0:10]

    return {'products': list(products_list), 'numbers': list(numbers)}

@router.post('/days_hours')
async def days_hours(file: UploadFile = File(...)):
    try:
        file_name, file_extension = os.path.splitext(file.filename)
        if file_extension == ".xlsx":
            df = pd.read_excel(file.file._file)
        else :
            df = pd.read_csv(file.file._file)
    except:
        raise exeptions.not_valid_file

    df = preprocessing.filter_data(df)

    df['Invoicehour'] = df['InvoiceDate'].apply(lambda x: x.hour)

    def search_1c(df, column1, value1):
        return df[df[column1] == value1]
    
    x = range(0,24)
    x = list(x)
    y = []
    for i in x:
        count = int(search_1c(df, 'Invoicehour', i)['Invoicehour'].count())
        y.append(count)

    return {'hours': x, 'counts': y}

    