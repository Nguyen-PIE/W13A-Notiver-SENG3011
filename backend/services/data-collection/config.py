# configs
# This file contains constants and configurations used across the application.

VERSION="v1"

# == collection microservice ==

# raw column names from the Excel file
RAW_LGA_COL = "Local Government Area"
RAW_OFFENCE_COL = "Offence type"
RAW_RATE_COL = "Rate per 100,000 population Oct 2024 - Sep 2025"
RAW_TREND_COL = "10 year trend and average annual percent change (Oct 2015-Sep 2025)"

# for processing rows
CHUNK_SIZE = 2000 
PD_ENGINE = 'calamine'
DEFAULT_TIMEZONE = "GMT+11"