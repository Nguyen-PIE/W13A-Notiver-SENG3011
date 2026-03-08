import boto3
import pandas as pd
from fastapi import APIRouter, UploadFile, File, HTTPException
from datetime import date

from models import Event, CrimeDataExport

router = APIRouter()
s3_client = boto3.client('s3')

# TO DO
BUCKET_NAME = "your-data-lake-bucket"

@router.get("/")
def root():
    return {"Welcome to Notiver's homepage!"}

@router.post("/process-data")
def process_excel(my_file: UploadFile = File(...)):
    if not my_file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload an Excel file.")

    try:
        # df = pd.read_excel(my_file.file, engine='calamine')
        df_iterator = pd.read_excel(my_file.file, engine='calamine', chunksize=2000)

        all_events = []
    
        # for chunk in df_iterator:
        #     # Clean NaN for Pydantic
        #     chunk = chunk.where(pd.notnull(chunk), None)
            
        #     for _, row in chunk.iterrows():
        #         try:
        #             # Map row to Event model
        #             event = Event(**row.to_dict())
        #             all_events.append(event)
        #         except Exception:
        #             continue

        # 2. Construct the Master JSON Object
        final_output = CrimeDataExport(
            data_source="BOSCAR Data",
            data_type="Dataset",
            collection_time=date.today().strftime("%Y/%m/%d"),
            events=all_events
        )
        return final_output.model_dump_json()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error parsing Excel: {str(e)}")
    
    # finally:
    #     buffer.close()

@router.post("/process-news")
def post_news():
    return {"data": "Data created successfully!"}

