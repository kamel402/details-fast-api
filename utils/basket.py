import pandas as pd
import re
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.frequent_patterns import association_rules
import warnings
warnings.filterwarnings("ignore")


def generate_products(df):
    df = df[['CustomerID', 'CustmerName', 'OrderState',
             'InvoiceNo', 'InvoiceDate', 'amount', 'products']]

    new_cs_df = pd.DataFrame(
        columns=['InvoiceNo', 'CustomerID', 'CustmerName', 'products', 'Quantity'])

    pattern1 = "\(SKU:(.*?)\)"
    pattern2 = "\(Qty: (.*?)\)"
    pattern3 = "KU:(.*?)\)"
    for index, row in df.iterrows():
        s = str(row['products']).split(', (S')
        for item in s:
            substirng1 = re.search(pattern1, item)
            if substirng1:
                substirng1 = re.search(pattern1, item).group(0)
                item = item.replace(substirng1, '')

            substirng3 = re.search(pattern3, item)
            if substirng3:
                substirng3 = re.search(pattern3, item).group(0)
                item = item.replace(substirng3, '')

            substring2 = re.search(pattern2, item).group(1)
            quantity = int(substring2)
            substring2 = re.search(pattern2, item).group(0)
            item = item.replace(substring2, '')

            item = item.strip()
            d = {'InvoiceNo': row['InvoiceNo'], 'CustomerID': row['CustomerID'],
                 'CustmerName': row['CustmerName'], 'products': item, 'Quantity': quantity}

            new_cs_df = new_cs_df.append(d, ignore_index=True)

    new_cs_df.dropna(subset=['products'], inplace=True)
    new_cs_df = new_cs_df[new_cs_df['products'] != '']

    products = new_cs_df.groupby(['InvoiceNo', 'CustomerID', 'CustmerName', 'products']).agg(
        {'Quantity': 'sum'}).reset_index()
    products.to_csv("products.csv")

    return products


def generate_transactions(df):
    items = list(df.products.unique())
    grouped = df.groupby('InvoiceNo')
    transaction_level = grouped.aggregate(lambda x: tuple(x)).reset_index()[
        ['InvoiceNo', 'products']]
    transaction_dict = {item: 0 for item in items}
    output_dict = dict()
    temp = dict()
    for rec in transaction_level.to_dict('records'):
        invoice_num = rec['InvoiceNo']
        items_list = rec['products']
        transaction_dict = {item: 0 for item in items}
        transaction_dict.update(
            {item: 1 for item in items if item in items_list})
        temp.update({invoice_num: transaction_dict})

    new = [v for k, v in temp.items()]
    transaction_df = pd.DataFrame(new)

    return transaction_df


def calculate_frequent_itemsets(transaction_df, min_support=0.003):
    frequent_itemsets = fpgrowth(
        transaction_df, min_support=min_support, use_colnames=True)
    return frequent_itemsets


def to_list(row):
    return list(row)


def generate_rules(frequent_itemsets, min_threshold=0.3):
    rules = association_rules(
        frequent_itemsets, metric="confidence", min_threshold=min_threshold)
    rules = rules.sort_values(
        ['support', 'confidence', 'lift'], ascending=False)
    rules = rules.reset_index()
    rules.drop(['index'], axis=1, inplace=True)
    rules = rules[['antecedents', 'consequents', 'support', 'confidence']]
    rules['antecedents'] = rules['antecedents'].apply(lambda row: to_list(row))
    rules['consequents'] = rules['consequents'].apply(lambda row: to_list(row))
    return rules
