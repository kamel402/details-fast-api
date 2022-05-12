import pandas as pd

def calculate_invoice_hour(df):
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['InvoiceHour'] = df['InvoiceDate'].apply(lambda x: x.hour)
    return df

def calculate_frequency(df):
    customer_history_df =  df[['CustomerID', 'CustmerName']].groupby("CustomerID").min().reset_index()
    customer_freq = (df[['CustomerID', 'InvoiceNo']].groupby(["CustomerID", 'InvoiceNo']).count().reset_index()).groupby(["CustomerID"]).count().reset_index()
    customer_freq.rename(columns={'InvoiceNo':'frequency'},inplace=True)
    customer_history_df = customer_history_df.merge(customer_freq)

    df['isMorning'] = df.apply (lambda row: morning_label(row), axis=1)

    df['isNight'] = df.apply (lambda row: night_label(row), axis=1)

    new_df = df.groupby(['CustomerID'])['isMorning'].agg('sum').reset_index()
    new_df.rename(columns={'isMorning':'morning_purchase'},inplace=True)
    customer_history_df = customer_history_df.merge(new_df)

    new_df = df.groupby(['CustomerID'])['isNight'].agg('sum').reset_index()
    new_df.rename(columns={'isNight':'night_purchase'},inplace=True)
    customer_history_df = customer_history_df.merge(new_df)

    return customer_history_df

def morning_label(row):
    if row['InvoiceHour'] >= 0 and row['InvoiceHour'] < 12:
        return 1
    else:
        return 0

def night_label(row):
    if row['InvoiceHour'] >= 12:
        return 1
    else:
        return 0

def category_label(row):
    if row['morning_purchase'] > row['night_purchase']:
        return 'صباح'
    elif  row['morning_purchase'] == row['night_purchase']:
        return 'محايد'
    else:
        return 'مساء'

def calculate_time_segment(df):
    df['Time_segment'] = df.apply (lambda row: category_label(row), axis=1)
    return df

def drop_unnecessary_column(df):
    df.drop(["InvoiceDate"],axis=1, inplace=True)
    df.drop(["InvoiceHour"],axis=1, inplace=True)
    df.drop(["InvoiceNo"],axis=1, inplace=True)
    return df