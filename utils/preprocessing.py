import pandas as pd



def filter_data(df):
    df = df[df['amount'] != 0]

    df = df[df['OrderState'] != 'مسترجع']
    df = df[df['OrderState'] != 'ملغي']
    df = df[df['OrderState'] != 'بإنتظار المراجعة']

    #df.dropna(subset = ['products'], inplace = True)

    df.dropna(subset = ['payment_method'], inplace = True)

    df['InvoiceDate']= pd.to_datetime(df['InvoiceDate'])

    return df