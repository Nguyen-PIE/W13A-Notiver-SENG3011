from fastapi import FastAPI
from app.api.routes import router
from mangum import Mangum

app = FastAPI(title="Notiver NLP Processing API")
app.include_router(router)
handler = Mangum(app)