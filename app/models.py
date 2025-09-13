from pydantic import BaseModel, Field, validator
from typing import Optional, Any
from datetime import datetime
import json

# optional is used for fields that can be None
# Any is used for fields that can be of any type

class TaskCreate(BaseModel):
    title: str = Field(..., min_length = 3, max_length = 100)
    payload: Optional[dict] = Field(default_factory = dict) # default_factory means if not provided, it will be an empty dict. 
    priority: Optional[int] = Field(default = 5, ge = 1, le = 10) # ge means greater than or equal to, le means less than or equal to. 

class TaskResponse(BaseModel):
    id: int
    title: str 
    status: str
    created_at: str 
    payload: Optional[dict]
    result: Optional[Any] 

class Task:

    # Simulating a database model with a simple class
    # id_ is used instead of id to avoid conflict with built-in id() function in Python.
    # Overall, task model is used to represent a task in the system, including its attributes and methods to convert to/from database rows.

    def __init__(self, id_: int, title: str, payload: dict, priority: int = 5, status: str = "pending", result: Optional[Any] = None,
                 created_at: Optional[str] = None):
        self.id = id_
        self.title = title
        self.payload = payload or {}
        self.priority = priority
        self.status = status
        self.created_at = created_at or datetime.utcnow().isoformat()
        self.result = result
    
    # to_row() method to convert Task object to a tuple for database insertion
    # json.dumps() is needed for payload and result to store them as JSON strings in the database.

    def to_row(self):
        return (self.id, self.title, self.status, json.dumps(self.payload), json.dumps(self.result), self.created_at, self.priority)

    # from_row() method to create a Task object from a database row (tuple)

    def from_row(row):
        id_, title, status, payload_json, result_json, created_at, priority = row 
        payload = json.loads(payload_json) if payload_json else {}
        result = json.loads(result_json) if result_json else None
        return Task(id_, title, payload, priority, status, result, created_at)

    
