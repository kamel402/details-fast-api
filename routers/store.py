from fastapi import APIRouter, UploadFile, File
import pandas as pd
import numpy as np

from utils import exeptions, preprocessing

router = APIRouter(
    prefix='/store',
    tags=['store level']
)


@router.post('/')
async def store_level_mining(file: UploadFile = File(...)):
    try:
        df = pd.read_excel(file.file._file)
    except:
        raise exeptions.not_valid_file
    
    df = preprocessing.filter_data(df)

    fav_payment = df['payment_method'].value_counts() / df.shape[0] * 100
               
    fav_payment = np.array([fav_payment.index,fav_payment.astype(float)])
    fav_payment = np.transpose(fav_payment, axes=None)

    return {'favorite payment': fav_payment}



