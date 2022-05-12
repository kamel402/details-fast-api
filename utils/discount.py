def calculate_dicount(df):
    customer_history_df =  df[['CustomerID', 'CustmerName']].groupby("CustomerID").min().reset_index()
    customer_freq = (df[['CustomerID', 'InvoiceNo']].groupby(["CustomerID", 'InvoiceNo']).count().reset_index()).groupby(["CustomerID"]).count().reset_index()
    customer_freq.rename(columns={'InvoiceNo':'frequency'},inplace=True)
    customer_history_df = customer_history_df.merge(customer_freq)

    df['isDiscount'] = df.apply (lambda row: label_race(row), axis=1)

    new_df = df.groupby(['CustomerID'])['isDiscount'].agg('sum').reset_index()
    new_df.rename(columns={'isDiscount':'number_of_discounts'},inplace=True)
    customer_history_df = customer_history_df.merge(new_df)

    customer_discount_val = df[['CustomerID', 'discount']].groupby("CustomerID").sum().reset_index()
    customer_history_df = customer_history_df.merge(customer_discount_val)

    customer_discount_val = df[['CustomerID', 'amount']].groupby("CustomerID").sum().reset_index()
    customer_history_df = customer_history_df.merge(customer_discount_val)

    customer_history_df['percentege'] = (customer_history_df['number_of_discounts']/customer_history_df['frequency'])*100

    print(customer_history_df['percentege'].max())

    return customer_history_df

def label_race(row):
    if row['discount'] > 0:
        return 1
    else:
        return 0

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






