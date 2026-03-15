from fastapi import APIRouter, HTTPException
from app.services.processor import run_nlp_pipeline

router = APIRouter()

@router.get("/")
def root():
    return {"message": "Data Processing Service is running"}

@router.post("/process-articles")
def process_articles():
    try:
        result = run_nlp_pipeline()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pipeline error: {str(e)}")