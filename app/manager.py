import sqlite3
import threading
import json
from .models import Task
from typing import List, Optional

DB_PATH = "tasks.db"

class TaskManager:
    def __init__(self, db_path = DB_PATH):
        self.db_path = db_path
        self.__lock = threading.Lock() # __lock is a private attribute to ensure thread safety when accessing the database connection and cursor objects. This prevents concurrent access to the database and ensures that only one thread can execute database operations at a time.
        self._ensure_table() # _ensure_table is a protected method to create the tasks table if it doesn't exist. For example, subclasses can override this method to customize table creation. This method is not intended to be accessed directly from outside the class. It is called automatically when an instance of TaskManager is created.

    def _get_conn(self):

        # _get_conn is a protected method to establish and return a connection to the SQLite database. The check_same_thread=False parameter allows the connection to be used across multiple threads, which is important for thread safety in a multi-threaded environment. 

        conn = sqlite3.connect(self.db_path, check_same_thread = False) # check_same_thread=False allows the connection to be used across multiple threads. 
        return conn 
    
    def _ensure_table(self):
        with self.__lock: # __lock is a private attribute to ensure thread safety when accessing the database connection and cursor objects. This prevents concurrent access to the database and ensures that only one thread can execute database operations at a time.
            conn = self._get_conn()
            cursor = conn.cursor() # cursor is a local variable to execute SQL commands and queries on the database. It is created from the connection object and is used to interact with the database.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload TEXT,
                    result TEXT,
                    created_at TEXT,
                    priority INTEGER DEFAULT 5
                )
            """)
            conn.commit()
            conn.close()

    def create_task(self, title: str, payload: dict, priority: int = 5) -> Task:
        with self.__lock:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute("INSERT INTO tasks (title, status, payload, result, created_at, priority) VALUES (?, ?, ?, ?, datetime('now'), ?)", (title, "pending", json_dump(payload), json_dump(None), priority)) # json_dump is a function to serialize a Python object (like a dictionary) into a JSON-formatted string. This is useful for storing complex data structures in a database, as SQLite does not have a native JSON data type. By converting the payload and result to JSON strings, we can easily store and retrieve them from the database.
            task_id = cur.lastrowid # lastrowid is a property of the cursor object that returns the row ID of the last inserted row. This is useful for obtaining the unique identifier of a newly created task, which can be used for further operations like updating or retrieving the task.
            conn.commit()
            conn.close()
            return Task(task_id, title, payload, priority, "pending", None) 
    
    def list_tasks(self) -> List[Task]:
        with self.__lock:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT id, title, status, payload, result, created_at, priority FROM tasks ORDER BY id ASC")
            rows = cursor.fetchall() # fetchall is a method of the cursor object that retrieves all rows from the result set of a query. It returns a list of tuples, where each tuple represents a row in the database. This is useful for obtaining all tasks from the tasks table and processing them further.
            conn.close()
        return [Task.from_row(r) for r in rows] # from_row is a class method of the Task class that creates a Task instance from a database row. It takes a tuple representing a row from the tasks table and maps the values to the corresponding attributes of the Task class. This method is useful for converting database query results into Task objects that can be easily manipulated in Python.

    def get_task(self, task_id: int) -> Optional[Task]:
        with self.__lock:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute("SELECT id, title, status, payload, result, created_at, priority FROM tasks WHERE id = ?", (task_id,))
            row = cursor.fetchone() # fetchone is a method of the cursor object that retrieves the next row from the result set of a query. It returns a single tuple representing the row, or None if there are no more rows. This is useful for obtaining a specific task by its ID from the tasks table.
            conn.close()
        return Task.from_row(row) if row else None
    
    def update_task_status_and_result(self, task_id: int, status: str, result: dict):
        with self.__lock:
            conn = self._get_conn()
            cur = conn.cursor()
            cursor.execute("UPDATE tasks SET status = ?, result = ? WHERE id = ?", (status, json_dump(result), task_id))
            conn.commit()
            conn.close()

def json_dump(obj):
    return json.dumps(obj, default = str) # default=str is used to handle non-serializable objects like datetime. It converts them to strings during JSON serialization. 