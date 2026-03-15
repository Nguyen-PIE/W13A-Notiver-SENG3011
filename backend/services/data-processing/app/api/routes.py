from fastapi import APIRouter, HTTPException
from app.services.processor import run_nlp_pipeline, fetch_processed_data

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

@router.get("/processed-articles")
def get_processed_articles():
    try:
        data = fetch_processed_data()
        
        if isinstance(data, dict) and "error" in data:
            raise HTTPException(status_code=404, detail=data["error"])
            
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fetch error: {str(e)}")