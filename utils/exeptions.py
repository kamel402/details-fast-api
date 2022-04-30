from fastapi import HTTPException


not_valid_file = HTTPException(
            status_code=404,
            detail='Not valid file'
        )