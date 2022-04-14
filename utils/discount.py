def calculate_frequency(df):
    customer_history_df =  df[['CustmerName', 'CustomerID', 'InvoiceNo', 'amount', 'discount']].groupby("CustomerID").min().reset_index()
    customer_freq = (df[['CustomerID', 'InvoiceNo']].groupby(["CustomerID", 'InvoiceNo']).count().reset_index()).groupby(["CustomerID"]).count().reset_index()
    customer_freq.rename(columns={'InvoiceNo':'frequency'},inplace=True)
    customer_history_df = customer_history_df.merge(customer_freq)

    return customer_history_df

def label_race(row):
    if row['discount'] > 0:
        return 1
    else:
        return 0

def is_discount(df):
    df['isDiscount'] = df.apply (lambda row: label_race(row), axis=1)
    return df

def calculate_number_of_discounts(df):
    new_df = df.groupby(['CustomerID'])['isDiscount'].agg('sum').reset_index()
    new_df.rename(columns={'isDiscount':'number_of_discounts'},inplace=True)
    customer_history_df = df.merge(new_df)
    return customer_history_df

def calculate_amount_of_discount(df):
    customer_discount_val = df[['CustomerID', 'discount']].groupby("CustomerID").sum().reset_index()
    customer_history_df = df.merge(customer_discount_val)
    return customer_history_df

def calculate_amount(df):
    customer_discount_val = df[['CustomerID', 'amount']].groupby("CustomerID").sum().reset_index()
    customer_history_df = df.merge(customer_discount_val)
    return customer_history_df

def calculate_discounts_percentege(df):
    df['percentege'] = (df['number_of_discounts']/df['frequency'])*100
    return df

def category_label(row):
    if row['percentege'] >= 70 and row['frequency'] > 1:
        return "مهتم بالعروض اشترى اكثر من مرة"
    elif row['percentege'] >= 70:
        return 'مهتم بالعروض اشترى مرة وحدة فقط'
    else:
        return 'عادي'

def calculate_discount_segment(df):
    df['Discount_segment'] = df.apply (lambda row: category_label(row), axis=1)
    return df

def drop_unnecessary_column(df):
    df.drop(["discount"],axis=1, inplace=True)
    df.drop(["InvoiceNo"],axis=1, inplace=True)
    return df






