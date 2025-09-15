from fastapi import FastAPI, HTTPException, UploadFile, File, BackgroundTasks
from fastapi.responses import JSONResponse
from .models import TaskCreate, TaskResponse
from .manager import TaskManager
from .worker import Worker
from .utils import summary_json
import csv
import io 
import json 
import asyncio 

# Define the FastAPI app and task manager and worker objects as global variables. 

app = FastAPI(title = "Batch Task Processor")
manager = TaskManager()
worker = Worker(manager)

@app.get("/")
def root():
    return {"message": "Welcome to the Stock Price Project API!"}

@app.post("/tasks", response_model = TaskResponse)
async def create_task(task_in: TaskCreate):

# Create a task using the TaskManager and Worker objects.
# Return the created task as a TaskResponse object.
# Use asyncio to run the task creation in a separate thread. get_event_loop is used to get the event loop, and run_in_executor is used to run the task creation in a separate thread.

    loop = asyncio.get_event_loop
    task = await loop.run_in_executor(None, manager.create_task, task_in.title, task_in.payload, task_in.priority)
    return TaskResponse(
        id = task.id,
        title = task.title,
        status = task.status,
        created_at = task.created_at,
        payload = task.payload,
        result = task.result
    )

@app.get("/tasks")
async def list_tasks():
    loop = asyncio.get_event_loop()
    tasks = await loop.run_in_executor(None, manager.list_tasks)
    return [TaskResponse(id = t.id, title = t.title, status = t.status, created_at = t.created_at, payload = t.payload, result = t.result) for t in tasks]

@app.get("/tasks/{task_id}")
async def get_task(task_id: int):
    loop = asyncio.get_event_loop()
    task = await loop.run_in_executor(None, manager.get_task, task_id)
    if not task:
        raise HTTPException(status_code = 404, detail = "Task not found")
    return TaskResponse(id = task.id, title = task.title, status = task.status, created_at = task.created_at, payload = task.payload, result = task.result)

@app.post("/tasks/{task_id}/enqueue")
async def enqueue_task(task_id: int):
    loop = asyncio.get_event_loop()
    task = await loop.run_in_executor(None, manager.get_task, task_id)
    if not task:
        raise HTTPException(status_code = 404, detail = "Task not found")
    worker.submit(task_id)
    return {"ok": True, "queued_task_id": task_id}

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    contents = await file.read()
    s = contents.decode()
    reader = csv.DictReader(io.StringIO(s))
    created_ids = []
    loop = asyncio.get_event_loop()
    for row in reader:
        title = row.get("title") or "untitled"
        numbers_raw = row.get("numbers", "")
        numbers = []
        if numbers_raw:
            numbers = [float(x) for x in numbers_raw.split(",") if x.strip() != ""]
        payload = {"numbers": numbers}
        task = await loop.run_in_executor(None, manager.create_task, title, payload, 5)
        created_ids.append(task.id)
        worker.submit(task.id)
    return {"created_task_ids": created_ids}

@app.get("/report")
async def report():
    loop = asyncio.get_event_loop()
    summary = await loop.run_in_executor(None, summary_json, manager)
    return JSONResponse(content = summary)

@app.on_event("shutdown")
def shutdown_event():
    worker.stop() 
         
