import pandas as pd
import datetime
import math
import numpy as np


def calculate_rfm(df):
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    refrence_date = df['InvoiceDate'].max() + datetime.timedelta(days=1)
    df['days_since_last_purchase'] = (
        refrence_date - df['InvoiceDate']).astype('timedelta64[D]')
    customer_history_df = df[['CustomerID', 'CustmerName', 'days_since_last_purchase']].groupby(
        "CustomerID").min().reset_index()
    customer_history_df.rename(
        columns={'days_since_last_purchase': 'recency'}, inplace=True)

    customer_freq = (df[['CustomerID', 'InvoiceNo']].groupby(["CustomerID", 'InvoiceNo']).count(
    ).reset_index()).groupby(["CustomerID"]).count().reset_index()
    customer_freq.rename(columns={'InvoiceNo': 'frequency'}, inplace=True)
    customer_history_df = customer_history_df.merge(customer_freq)

    customer_monetary_val = df[['CustomerID', 'amount']].groupby(
        "CustomerID").sum().reset_index()
    customer_history_df = customer_history_df.merge(customer_monetary_val)

    return customer_history_df


def calculate_rfm_logs(df):
    df['recency_log'] = df['recency'].apply(math.log)
    df['frequency_log'] = df['frequency'].apply(math.log)
    df['amount_log'] = df['amount'].apply(math.log)

    return df


def devide_into_4_categories(df, column, new_column_name):
    max = df[column].max()
    min = df[column].min()

    period = (max - min) / 4

    cat1 = (min, min+period)
    cat2 = (cat1[1], cat1[1]+period)
    cat3 = (cat2[1], cat2[1]+period)
    cat4 = (cat3[1], cat3[1]+period)

    def category_label(row):
        if row[column] >= cat1[0] and row[column] < cat1[1]:
            return int(1)
        if row[column] >= cat2[0] and row[column] < cat2[1]:
            return int(2)
        if row[column] >= cat3[0] and row[column] < cat3[1]:
            return int(3)
        if row[column] >= cat4[0] and row[column] <= cat4[1]:
            return int(4)

    df[new_column_name] = df.apply(lambda row: category_label(row), axis=1)


def devide_into_4_categories_minmax(df, column, new_column_name, max, min):

    period = (max - min) / 4

    cat1 = (min, min+period)
    cat2 = (cat1[1], cat1[1]+period)
    cat3 = (cat2[1], cat2[1]+period)
    cat4 = (cat3[1], cat3[1]+period)

    def category_label(row):
        if row[column] >= cat1[0] and row[column] < cat1[1]:
            return int(1)
        if row[column] >= cat2[0] and row[column] < cat2[1]:
            return int(2)
        if row[column] >= cat3[0] and row[column] < cat3[1]:
            return int(3)
        if row[column] >= cat4[0] and row[column] <= cat4[1]:
            return int(4)

    df[new_column_name] = df.apply(lambda row: category_label(row), axis=1)


def devide_into_4_categoriesRecency(df, column, new_column_name):
    max = df[column].max()
    min = df[column].min()

    period = (max - min) / 4

    cat1 = (min, min+period)
    cat2 = (cat1[1], cat1[1]+period)
    cat3 = (cat2[1], cat2[1]+period)
    cat4 = (cat3[1], cat3[1]+period)

    def category_label(row):
        if row[column] >= cat1[0] and row[column] < cat1[1]:
            return int(4)
        if row[column] >= cat2[0] and row[column] < cat2[1]:
            return int(3)
        if row[column] >= cat3[0] and row[column] < cat3[1]:
            return int(2)
        if row[column] >= cat4[0] and row[column] <= cat4[1]:
            return int(1)

    df[new_column_name] = df.apply(lambda row: category_label(row), axis=1)


def devide_into_4_categoriesRecency_minmax(df, column, new_column_name, max, min):

    period = (max - min) / 4

    cat1 = (min, min+period)
    cat2 = (cat1[1], cat1[1]+period)
    cat3 = (cat2[1], cat2[1]+period)
    cat4 = (cat3[1], cat3[1]+period)

    def category_label(row):
        if row[column] >= cat1[0] and row[column] < cat1[1]:
            return int(4)
        if row[column] >= cat2[0] and row[column] < cat2[1]:
            return int(3)
        if row[column] >= cat3[0] and row[column] < cat3[1]:
            return int(2)
        if row[column] >= cat4[0] and row[column] <= cat4[1]:
            return int(1)

    df[new_column_name] = df.apply(lambda row: category_label(row), axis=1)


def calculate_4_categories(df):
    devide_into_4_categoriesRecency(df, 'recency_log', 'R')
    devide_into_4_categories(df, 'frequency_log', 'F')
    devide_into_4_categories(df, 'amount_log', 'M')
    return df


def calculate_4_categories_minmax(df, minmax):
    devide_into_4_categoriesRecency_minmax(
        df, 'recency_log', 'R', minmax[0], minmax[1])
    devide_into_4_categories_minmax(
        df, 'frequency_log', 'F', minmax[2], minmax[3])
    devide_into_4_categories_minmax(
        df, 'amount_log', 'M', minmax[4], minmax[5])
    return df


def rfm_segment(df):
    df['RFM_segment'] = df.apply(lambda row: str(
        int(row['R']))+str(int(row['F']))+str(int(row['M'])), axis=1)
    return df


def rfm_score(df):
    df['RFM_score'] = df.apply(lambda row: int(
        row['RFM_segment'][0])+int(row['RFM_segment'][1])+int(row['RFM_segment'][2]), axis=1)
    df = df.astype({"RFM_segment": int})
    return df


def category_label(row):
    if row['RFM_score'] > 3 and row['RFM_score'] <= 5:
        return 'Low'
    if row['RFM_score'] >= 6 and row['RFM_score'] <= 8:
        return 'Middle'
    if row['RFM_score'] >= 9 and row['RFM_score'] <= 12:
        return 'Top'


def calculate_general_segment(df):
    df['general_segment'] = df.apply(lambda row: category_label(row), axis=1)
    return df

# def calculate_city(df):
#     df.city.fillna(value='NaN', inplace=True)
#     city_df = df[['CustomerID','city']].groupby(['CustomerID']).agg(lambda x:x.value_counts().index[0]).reset_index()
#     df.drop(['city'], axis=1, inplace=True)
#     city_df = city_df[['CustomerID','city']]
#     df = df.merge(city_df)
#     return df

# def calculate_payment_method(df):
#     payment_df = df[['CustomerID','payment_method']].groupby(['CustomerID']).agg(lambda x:x.value_counts().index[0]).reset_index()
#     df.drop(['payment_method'], axis=1, inplace=True)
#     payment_df = payment_df[['CustomerID','payment_method']]
#     df = df.merge(payment_df)
#     return df


def drop_unnecessary_columns(df):
    return df[['CustomerID', 'CustmerName', 'recency', 'frequency', 'amount', 'RFM_score', 'general_segment']]


def find_max_min(cs_df):
   # R
    customer_history_df = cs_df[['CustomerID', 'CustmerName', 'days_since_last_purchase']].groupby(
        "CustomerID").min().reset_index()
    customer_history_df.rename(
        columns={'days_since_last_purchase': 'recency'}, inplace=True)
    # F
    customer_freq = (cs_df[['CustomerID', 'InvoiceNo']].groupby(["CustomerID", 'InvoiceNo']).count().reset_index()).\
        groupby(["CustomerID"]).count().reset_index()
    customer_freq.rename(columns={'InvoiceNo': 'frequency'}, inplace=True)
    customer_history_df = customer_history_df.merge(customer_freq)
    # M
    customer_monetary_val = cs_df[['CustomerID', 'amount']].groupby(
        "CustomerID").sum().reset_index()
    customer_history_df = customer_history_df.merge(customer_monetary_val)
    # RFM logs
    customer_history_df['recency_log'] = customer_history_df['recency'].apply(
        math.log)
    customer_history_df['frequency_log'] = customer_history_df['frequency'].apply(
        math.log)
    customer_history_df['amount_log'] = customer_history_df['amount'].apply(
        math.log)

    maxR = customer_history_df['recency_log'].max()
    minR = customer_history_df['recency_log'].min()

    maxF = customer_history_df['frequency_log'].max()
    minF = customer_history_df['frequency_log'].min()

    maxM = customer_history_df['amount_log'].max()
    minM = customer_history_df['amount_log'].min()

    return [maxR, minR, maxF, minF, maxM, minM]
