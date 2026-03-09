# W13A-Notiver-SENG3011

This repository contains React frontend and FastAPI NLP backend we will spend the next 9 weeks contributing to.

## Tech Stack (v1)
* Frontend: Next.js (React), TypeScript, Tailwind CSS
* Backend: Python 3.10+, FastAPI, Uvicorn
* Package Managers: npm (Frontend), pip (Backend)

---

## Prerequisites
Before you start, ensure you have the following installed on your machine:
1. Git: To clone the repository.
2. Python 3.10+: Verify by running `python --version` (or `python3 --version` on Mac/Linux) in your terminal.
3. Node.js (v18+): Verify by running `node -v` in your terminal. This will automatically install npm.

---

## Getting Started

First, clone the repository to your local machine using either SSH or HTTPS:

Using SSH (Requires SSH keys setup):
```bash
git clone git@github.com:Nguyen-PIE/W13A-Notiver-SENG3011.git
```


## How to start the backend environment
```bash
cd W13A-Notiver-SENG3011
cd backend

# Create a virtual environment
# Mac/Linux:
python3 -m venv venv
# Windows:
python -m venv venv


# Activate the virtual environment
# Mac/Linux:
source venv/bin/activate
# Windows:
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## To run the Backend after starting the env
``` bash
uvicorn app.main:app --reload
```


## How To Run the Frontend
``` bash
cd frontend

# Install all Node dependencies
npm install

# Create your local environment variables file
# Open this file and add: NEXT_PUBLIC_API_URL=[http://127.0.0.1:8000](http://127.0.0.1:8000)
touch .env.local 

# Start the development server
npm run dev
```



## Daily Workflow
Every time you develop, you need two terminal windows open and running simultaneously:
Backend Terminal:

Navigate to:
```bash 
cd backend 
```

Activate the virtual environment:
```bash 
source venv/bin/activate
```

Run:
```bash
uvicorn app.main:app --reload
```

Frontend Terminal:
Navigate to 
```bash
cd frontend
```

Run:
```bash
npm run dev
```

---

## Testing and Coverage
As mentioned in the Engineering Proposal, Pytest is used as the architecture for testing.

To install Pytest:
Run
```bash
pip install pytest pytest-cov
```

To run tests (without coverage):
Run
```bash
pytest
```

To run tests (with coverage):
Run
```bash
pytest --cov=. --cov-report=term-missing
```


## Linting
The CI/CD workflow uses Ruff for linting. Therefore, the instructions below also use Ruff for linting purposes.

To install Ruff:
Run
```bash
pip install ruff
```

To run linting:
Run
```bash
ruff check .
```


## CI/CD Pipeline Workflow
The pipeline will trigger in the following circumstances:
* Creating a pull (merge) request with main
* Pulling (merging) into main

The pipeline checks for the following:
* All tests pass (using Pytest)
* Tests cover 100% of the code (using pytest-coverage)
* Linting (using Ruff)

After the pipeline runs, in addition to the information presented in output, a coverage report will be generated and viewable from GitHub Actions:
```bash
Actions > Click on the appropriate workflow run > Artifacts > Download the "coverage-html" artifact (ZIP file) > Unzip file > Open "index.html" to view coverage report
```
