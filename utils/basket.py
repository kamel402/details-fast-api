import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.frequent_patterns import association_rules

def generate_transactions(df):
    items = list(df.products.unique())
    grouped = df.groupby('InvoiceNo')
    transaction_level = grouped.aggregate(lambda x: tuple(x)).reset_index()[['InvoiceNo','products']]
    transaction_dict = {item:0 for item in items}
    output_dict = dict()
    temp = dict()
    for rec in transaction_level.to_dict('records'):
        invoice_num = rec['InvoiceNo']
        items_list = rec['products']
        transaction_dict = {item:0 for item in items}
        transaction_dict.update({item:1 for item in items if item in items_list})
        temp.update({invoice_num:transaction_dict})

    new = [v for k,v in temp.items()]
    transaction_df = pd.DataFrame(new)
    
    return transaction_df

def calculate_frequent_itemsets(transaction_df, min_support=0.003):
    frequent_itemsets = fpgrowth(transaction_df, min_support=min_support, use_colnames=True)
    return frequent_itemsets

def to_list(row):
    return list(row)

def generate_rules(frequent_itemsets, min_threshold=0.3):
    rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=min_threshold)
    rules = rules.sort_values(['support', 'confidence', 'lift'],ascending=False)
    rules = rules.reset_index()
    rules.drop(['index'], axis=1, inplace=True)
    rules = rules[['antecedents','consequents', 'support', 'confidence']]
    rules['antecedents'] =  rules['antecedents'].apply(lambda row: to_list(row))
    rules['consequents'] =  rules['consequents'].apply(lambda row: to_list(row))    
    return rules