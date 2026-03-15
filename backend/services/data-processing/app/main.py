from fastapi import FastAPI
from app.api.routes import router

app = FastAPI(title="Notiver NLP Processing API")
app.include_router(router)