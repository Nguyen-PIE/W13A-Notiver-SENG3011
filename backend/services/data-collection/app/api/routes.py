import io
from fastapi import APIRouter, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
from services.process_excel import process_data
from database.s3 import upload_fileobj_to_s3, collect_data
import config as config

router = APIRouter()

@router.get("/")
def root():
    return {"Welcome to Notiver's homepage!"}

@router.post("/upload-data")
def upload_data(my_file: UploadFile = File(...)):
    json_data = process_data(my_file)
    buffer = io.BytesIO(json_data.encode('utf-8'))
    file_name = config.EXCEL_BUCKET_NAME + "/" + config.EXCEL_FILE_NAME
    try:
        upload_fileobj_to_s3(buffer, config.S3_BUCKET_NAME, file_name)
        return { "message": f"File '{file_name}' processed and uploaded successfully!" }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading to S3: {e}")

@router.get("/collect-data")
def get_data():
    try:
        file_name = config.EXCEL_BUCKET_NAME + "/" + config.EXCEL_FILE_NAME
        body_stream, content_type = collect_data(config.S3_BUCKET_NAME, file_name)
        return StreamingResponse(
            body_stream,
            media_type=content_type,
            headers={
                "Content-Disposition": f"attachment; filename={config.EXCEL_FILE_NAME}"
            }
        )
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Error finding file: {e}")

@router.post("/upload-articles")
def post_articles():
    return {"data": "Data created successfully!"}

@router.get("/collect-articles")
def get_articles():
    return {"data": "Data collected successfully!"}