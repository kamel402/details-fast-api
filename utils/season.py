import pandas as pd
from hijri_converter import Hijri, Gregorian


def calculate_frequency(df):
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    customer_history_df = df[['CustmerName', 'CustomerID', 'InvoiceNo', 'InvoiceDate']].groupby(
        "CustomerID").min().reset_index()
    customer_freq = (df[['CustomerID', 'InvoiceNo']].groupby(["CustomerID", 'InvoiceNo']).count(
    ).reset_index()).groupby(["CustomerID"]).count().reset_index()
    customer_freq.rename(columns={'InvoiceNo': 'frequency'}, inplace=True)
    customer_history_df = customer_history_df.merge(customer_freq)

    return customer_history_df


def convert_date_to_hijri(df):
    df['InvoiceDateHijri'] = df['InvoiceDate'].apply(
        lambda h: Gregorian(h.year, h.month, h.day).to_hijri())
    return df


def calculate_only_fitr_purchase(df):
    df['onlyFitr'] = df['InvoiceDateHijri'].apply(
        lambda row: 1 if row.month == 10 else 0)
    return df


def calculate_only_adha_purchase(df):
    df['onlyAdha'] = df['InvoiceDateHijri'].apply(
        lambda row: 1 if row.month == 12 else 0)
    return df


def both_eid(row, df):
    customerID = row.CustomerID
    record = df[df['CustomerID'] == customerID]
    if len(record[record['onlyFitr'] == 1]) >= 1 and len(record[record['onlyAdha'] == 1]) >= 1:
        return 1
    else:
        return 0


def calculate_both_eid_purchase(df):
    df['bothEid'] = df.apply(lambda row: both_eid(row, df), axis=1)
    return df


def calculate_all_season_purchase(df):
    df['allSeason'] = df['InvoiceDateHijri'].apply(
        lambda row: 1 if row.month != 12 and row.month != 10 else 0)
    return df


def rename_features(df):
    new_df = df.groupby(['CustomerID'], as_index=False).agg({'onlyFitr': 'sum',
                                                             'onlyAdha': 'sum',
                                                             'bothEid': 'sum',
                                                             'allSeason': 'sum', }).rename(columns={'onlyFitr': 'fitr_purchase',
                                                                                                    'onlyAdha': 'adha_purchase',
                                                                                                    'bothEid': 'both_eid_purchase',
                                                                                                    'allSeason': 'all_season_purchase'})
    df = df[['CustomerID', 'CustmerName', 'frequency']]
    customer_history_df = df.merge(new_df)
    return customer_history_df


def season_label(row):
    if row['fitr_purchase'] > row['adha_purchase'] and row['fitr_purchase'] > row['all_season_purchase']:
        return 'عيد الفطر'
    elif row['adha_purchase'] > row['fitr_purchase'] and row['adha_purchase'] > row['all_season_purchase']:
        return 'عيد الاضحى'
    else:
        return 'طوال العام'


def calculate_season_segment(df):
    df['season_segment'] = df.apply(lambda row: season_label(row), axis=1)
    return df
