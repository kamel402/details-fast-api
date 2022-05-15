from fastapi import HTTPException


not_valid_file = HTTPException(
            status_code=404,
            detail='Not valid file'
        )
wrong_segment = HTTPException(
            status_code=404,
            detail='wrong segment number'
        )