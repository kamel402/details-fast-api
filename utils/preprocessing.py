import pandas as pd
import io
from fastapi.responses import StreamingResponse



def filter_data(df):
    df = df[df['amount'] != 0]

    df = df[df['OrderState'] != 'مسترجع']
    df = df[df['OrderState'] != 'ملغي']
    df = df[df['OrderState'] != 'بإنتظار المراجعة']

    df.dropna(subset = ['products'], inplace = True)
    df.dropna(subset = ['payment_method'], inplace = True)
    
    df = df.reset_index(drop=True)

    df['InvoiceDate']= pd.to_datetime(df['InvoiceDate'])

    return df

def to_stream(df, file_name):  
    stream = io.StringIO()

    df.to_csv(stream, index = False)

    response = StreamingResponse(iter([stream.getvalue()]),media_type="text/csv")

    response.headers["Content-Disposition"] = f"attachment; filename={file_name}.csv"

    return response