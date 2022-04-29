import uvicorn ##ASGI
from fastapi import FastAPI
from routers import basket, discount, rfm, season, time

# 2. Create the app object
app = FastAPI()

app.include_router(basket.router)
app.include_router(discount.router)
app.include_router(rfm.router)
app.include_router(season.router)
app.include_router(time.router)


# 3. Index route, opens automatically on http://127.0.0.1:8000
@app.get('/')
async def index():
    return {'message': 'try to open http://127.0.0.1:8000/docs to try the API'}

# 4. Run the API with uvicorn
#    Will run on http://127.0.0.1:9000
if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=9000)
#uvicorn main:app --reload



