from fastapi import APIRouter, UploadFile, File
from typing import Optional
import pandas as pd
import os, io, json, datetime
import numpy as np



from utils import rfm, exeptions, preprocessing
import schemas.path

router = APIRouter(
    prefix='/rfm',
    tags=['classification']
)
# RFM Classification endpoint


@router.post('/')
async def rfm_classification(limit: Optional[int] = None, file: UploadFile = File(...)):
    try:
        file_name, file_extension = os.path.splitext(file.filename)
        if file_extension == ".xlsx":
            df = pd.read_excel(file.file._file)
        else :
            df = pd.read_csv(file.file._file)
    except:
        raise exeptions.not_valid_file

    df = preprocessing.filter_data(df)
    # Calculating the rfm
    df = rfm.calculate_rfm(df)
    # Normlizing the rfm
    df = rfm.calculate_rfm_logs(df)
    # Divide rfm into 4 categories
    df = rfm.calculate_4_categories(df)
    # Create quartiles named RFM_segment
    df = rfm.rfm_segment(df)
    # Calculate rfm score by summing up the RFM quartile metrics
    df = rfm.rfm_score(df)
    # Create a general segment
    df = rfm.calculate_general_segment(df)
    # Unite cities
    # df = rfm.calculate_city(df)
    # # Unite payment methods
    # df = rfm.calculate_payment_method(df)
    # Drop unnecessary columns
    # df = rfm.drop_unnecessary_columns(df)
    # Convert Dataframe to json format
    if limit == None:
        data = df.to_json(orient='records', force_ascii=False)
    else:
        data = df.head(limit).to_json(orient='records', force_ascii=False)

    data = json.loads(data)

    return data

@router.post('/count')
async def rfm_classification(file: UploadFile = File(...)):
    try:
        file_name, file_extension = os.path.splitext(file.filename)
        if file_extension == ".xlsx":
            df = pd.read_excel(file.file._file)
        else :
            df = pd.read_csv(file.file._file)
    except:
        raise exeptions.not_valid_file

    df = preprocessing.filter_data(df)
    # Calculating the rfm
    df = rfm.calculate_rfm(df)
    # Normlizing the rfm
    df = rfm.calculate_rfm_logs(df)
    # Divide rfm into 4 categories
    df = rfm.calculate_4_categories(df)
    # Create quartiles named RFM_segment
    df = rfm.rfm_segment(df)
    # Calculate rfm score by summing up the RFM quartile metrics
    df = rfm.rfm_score(df)
    # Create a general segment
    df = rfm.calculate_general_segment(df)
    # # Unite cities
    # df = rfm.calculate_city(df)
    # # Unite payment methods
    # df = rfm.calculate_payment_method(df)
    # Drop unnecessary columns
    df = rfm.drop_unnecessary_columns(df)

    df = df.groupby(['general_segment'],as_index=False).agg({'CustomerID': 'count'}).rename(columns = {'CustomerID': 'count'})

    # Convert Dataframe to json format
    data = df.to_json(orient='records', force_ascii=False)
    
    data = json.loads(data)

    return data

@router.post('/monthly')
async def rfm_classification(file: UploadFile = File(...)):
    try:
        file_name, file_extension = os.path.splitext(file.filename)
        if file_extension == ".xlsx":
            df = pd.read_excel(file.file._file)
        else :
            df = pd.read_csv(file.file._file)
    except:
        raise exeptions.not_valid_file

    df = preprocessing.filter_data(df)

    # calclaute days_since_last_purchase for all the records on the original data frame
    refrence_date = df.InvoiceDate.max() + datetime.timedelta(days = 1)
    df['days_since_last_purchase'] = (refrence_date - df.InvoiceDate).astype('timedelta64[D]')

    # set the days on the date to 1
    df['MonthDate'] = df['InvoiceDate'].dt.to_period('M').dt.to_timestamp()

    # find all the dates 
    months = df['MonthDate'].unique()



    # how many months do we have ?
    number_of_months = months.size

    minmax = rfm.find_max_min(df)

    low_counts = []
    Middle_counts = []
    hight_counts = []

    for i in range(number_of_months-1,-1,-1):
        mask = df['MonthDate'] <= months[i]
        masked_df = df.loc[mask]

        # Calculating the rfm
        masked_df = rfm.calculate_rfm(masked_df)
        # Normlizing the rfm
        masked_df = rfm.calculate_rfm_logs(masked_df)
        # Divide rfm into 4 categories
        masked_df = rfm.calculate_4_categories_minmax(masked_df, minmax)
        # Create quartiles named RFM_segment
        masked_df = rfm.rfm_segment(masked_df)
        # Calculate rfm score by summing up the RFM quartile metrics
        masked_df = rfm.rfm_score(masked_df)
        # Create a general segment
        masked_df = rfm.calculate_general_segment(masked_df)
        
        masked_df = masked_df.groupby(['general_segment'],as_index=False).agg({'CustomerID': 'count'}).rename(columns = {'CustomerID': 'count'})
        print(masked_df['count'].iloc[0])
        low_counts.append(int(masked_df['count'].iloc[0]))
        if masked_df.shape[0] > 1:
            Middle_counts.append(int(masked_df['count'].iloc[1]))
        else:
            Middle_counts.append(0)
        
        if masked_df.shape[0] > 2:
            hight_counts.append(int(masked_df['count'].iloc[2]))
        else:
            hight_counts.append(0)
        # hight_counts.append(int(masked_df['count'].iloc[2]))

    months = np.flip(months.astype(np.int64) // 10 ** 6)
    months = months.tolist()


    return {
        'low_counts':low_counts,
        'Middle_counts':Middle_counts,
        'hight_counts':hight_counts,
        'months':months
        }

@router.post('/download/{segment}')
async def rfm_classification(segment: int, file: UploadFile = File(...)):
    try:
        file_name, file_extension = os.path.splitext(file.filename)
        if file_extension == ".xlsx":
            df = pd.read_excel(file.file._file)
        else :
            df = pd.read_csv(file.file._file)
    except:
        raise exeptions.not_valid_file

    df = preprocessing.filter_data(df)
    # Calculating the rfm
    df = rfm.calculate_rfm(df)
    # Normlizing the rfm
    df = rfm.calculate_rfm_logs(df)
    # Divide rfm into 4 categories
    df = rfm.calculate_4_categories(df)
    # Create quartiles named RFM_segment
    df = rfm.rfm_segment(df)
    # Calculate rfm score by summing up the RFM quartile metrics
    df = rfm.rfm_score(df)
    # Create a general segment
    df = rfm.calculate_general_segment(df)
  
    if segment == 0:
        response = preprocessing.to_stream(df,'all_rfm')
    elif segment == 1:
        mask = df['general_segment'] == 'Low'
        df = df.loc[mask]
        response = preprocessing.to_stream(df,'Low_rfm')
    elif segment == 2:
        mask = df['general_segment'] == 'Middle'
        df = df.loc[mask]
        response = preprocessing.to_stream(df,'Middle_rfm')
    elif segment == 3:
        mask = df['general_segment'] == 'Top'
        df = df.loc[mask]
        response = preprocessing.to_stream(df,'Top_rfm')
    else:
        raise exeptions.wrong_segment

    return response