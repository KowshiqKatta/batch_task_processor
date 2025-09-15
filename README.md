Batch Task Processor
--------------------

1. Create virtual env and install:
   python -m venv .venv
   source .venv/bin/activate   # or .venv\Scripts\activate on Windows
   pip install -r requirements.txt

2. Run server:
   uvicorn app.main:app --reload

3. Use client demo:
   python client.py

Endpoints:
- POST /tasks  (body: {title, payload, priority})
- POST /upload-csv  (file with columns: title,numbers where numbers are semicolon-separated)
- POST /tasks/{id}/enqueue
- GET /tasks
- GET /tasks/{id}
- GET /report

What it demonstrates:
- FastAPI (async endpoints)
- Pydantic validation
- Python classes for business logic
- sqlite3 persistence
- Background multithreading worker + ThreadPoolExecutor
- pandas-based reporting
