#pip install fastapi uvicorn

# 1. Library imports
import uvicorn ##ASGI
from fastapi import FastAPI
from routers import basket, segmentaion

# 2. Create the app object
app = FastAPI()

app.include_router(segmentaion.router)
app.include_router(basket.router)

# 3. Index route, opens automatically on http://127.0.0.1:8000
@app.get('/')
async def index():
    return {'message': 'Hello, World'}

# 4. Run the API with uvicorn
#    Will run on http://127.0.0.1:9000
if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=9000)
#uvicorn main:app --reload



