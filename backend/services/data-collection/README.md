# Data Collection Service

This service runs a FastAPI application using Uvicorn.

## Configure AWS S3

If required, configure AWS using awscli.

## Running the Service

Make sure you are in the `app` directory when running the command.

To start the server with auto-reload on code changes, run:

```bash
# prod
uvicorn main:app
```

For development, use:

```bash
# dev
uvicorn main:app --reload
```
Uvicorn will watch for changes in the source directory and automatically reload the server.
Note the `--reload` flag is recommended for development only.


## Requirements

- Python 3.7+
- [Uvicorn](https://www.uvicorn.org/)
- [FastAPI](https://fastapi.tiangolo.com/)
- add here ...

Install dependencies:

```bash
pip install -r requirements.txt
```

## Project Structure

```
backend/services/data-collection/
├── main.py
├── README.md
├── requirements.txt
└── src
    ├── api/                # Contains API route definitions and endpoints
    ├── services/           # Business logic and service layer
    ├── utils/              # Utility functions and helpers
    ├── database/           # s3 logic and database layer
    ├── config.py           # Configuration settings for the service
    └── models.py           # Pydantic models and data schemas
```