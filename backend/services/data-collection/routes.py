import io
import json

import boto3
import re
from fastapi.responses import StreamingResponse
import pandas as pd
from fastapi import APIRouter, UploadFile, File, HTTPException
from datetime import date
import config as config

from models import Event, CrimeDataExport

router = APIRouter()

# for database
# s3_client = boto3.client('s3')
# BUCKET_NAME = "your-data-lake-bucket"

@router.get("/")
def root():
    return {"Welcome to Notiver's homepage!"}

"""Process the uploaded Excel file, clean and transform the data, and return it as JSON."""
@router.post("/process-data")
def process_excel(my_file: UploadFile = File(...)):
    if not my_file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload an Excel file.")

    all_events = []
    
    df = pd.read_excel(my_file.file, engine=config.PD_ENGINE, header=3, nrows=8060)
    
    # clean data
    df[config.RAW_OFFENCE_COL] = df[config.RAW_OFFENCE_COL].str.replace(r'\*', '', regex=True).str.strip()
    df[config.RAW_RATE_COL] = df[config.RAW_RATE_COL].astype(object).replace('nc', None)
    df[config.RAW_TREND_COL] = df[config.RAW_TREND_COL].astype(object).replace('nc', None)
    df = df.astype(object).where(pd.notnull(df), other=None)
    
    # melt data into years
    id_vars = [config.RAW_LGA_COL, config.RAW_OFFENCE_COL, config.RAW_RATE_COL, config.RAW_TREND_COL]
    year_cols = [col for col in df.columns if re.match(r'^Oct \d{4}\s*-\s*Sep \d{4}$', str(col).strip())][-5:]
    df_melted = df.melt(id_vars=id_vars, value_vars=year_cols, var_name='year_range', value_name='count')
    
    # create json objects
    for _, row in df_melted.iterrows():
        
        # Split logic and model creation
        start, end = row['year_range'].split(" - ")
        trend = parse_trend(row[config.RAW_TREND_COL])
        event = Event(
            lga=row[config.RAW_LGA_COL],
            offence_type=row[config.RAW_OFFENCE_COL],
            offence_count=row['count'],
            rate_per_100k=row[config.RAW_RATE_COL],
            ten_year_trend=trend['direction'],
            ten_year_percent_change=trend['percent'],
            time_object={
                "date_start": f"{start[-4:]}-10-01",
                "date_end": f"{end[-4:]}-09-30"
            }
        )
        all_events.append(event)

    # construct the main json object
    final_output = CrimeDataExport(
        data_source="BOSCAR Data",
        data_type="Dataset",
        version=config.VERSION,
        # dataset_id=random.randint(1000, 9999), # TODO
        time_object={
            "date_collected": date.today().strftime("%Y/%m/%d"),
            "timezone": config.DEFAULT_TIMEZONE
        },
        events=all_events
    )
    json_data = final_output.model_dump_json()

    return StreamingResponse(
        io.BytesIO(json_data.encode()),
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=crime_export.json"}
    )


@router.post("/process-news")
def post_news():
    return {"data": "Data created successfully!"}


"""helper fn - to move??"""
def parse_trend(value):
    if value is None or str(value).strip().lower() == 'nc':
        return {"direction": None, "percent": None}
    
    value = str(value).strip()
    
    if value.lower() == 'stable':
        return {"direction": "stable", "percent": 0.0}
    
    # Match "Up 6.7%" or "Down 3.5%"
    match = re.match(r'(Up|Down)\s+([\d.]+)%', value, re.IGNORECASE)
    if match:
        direction = match.group(1).lower()  # "up" or "down"
        percent = float(match.group(2))
        return {"direction": direction, "percent": percent}
    
    return {"direction": None, "percent": None}  # fallback