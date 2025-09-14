import threading
import queue
import time 
from concurrent.futures import ThreadPoolExecutor
from .manager import TaskManager
from .models import Task 
import pandas as pd 

class Worker:
    def __init__(self, manager: TaskManager, max_workers: int = 3):
        self.manager = manager
        self.queue = queue.Queue()
        self.executor = ThreadPoolExecutor(max_workers = max_workers)
        self._stop_event = threading.Event() # Event to signal stopping the worker thread and its executor.
        self._thread = threading.Thread(target=self._run_loop, daemon = True) # Daemon thread to run the worker loop. The daemon attribute is set to True, which means that the thread will terminate when the main program exits. target is the method to be executed in the thread. run_loop is the method that contains the main loop for processing tasks.
        self._thread.start() # Start the worker thread.
    
    def submit(self, task_id: int):
        self.queue.put(task_id) # Add a task ID to the queue for processing.
    
    def _run_loop(self):
        while not self._stop_event.is_set(): # Continue running until the stop event is set.
            try:
                task_id = self.queue.get(timeout = 0.5) # Wait for a task ID from the queue, with a timeout of 0.5 second.
            except queue.Empty:
                continue # If the queue is empty, continue to the next iteration of the loop.
            self.executor.submit(self._process_task, task_id) # Submit the task for processing to the thread pool executor.
    
    def _process_wrapper(self, task_id: int):
        task = self.manager.get_task(task_id) # Retrieve the task object using the task ID.
        if not task:
            return 
        self.manager.update_task_status_and_result(task_id, "processing", {"msg": "started"})
        try:
            result = self.process_task(task) # Process the task and get the result.
            self.manager.update_task_status_and_result(task_id, "done", result) 
        except Exception as e:
            self.manager.update_task_status_and_result(task_id, "failed", {"error": str(e)})
    
    def process_task(self, task: Task):
        payload = task.payload or {}
        if "numbers" in payload:
            nums = payload.get("numbers", [])
            df = pd.DataFrame({"n": nums})
            summary = {
                "count": int(df["n"].count()),
                "sum": float(df["n"].sum()),
                "mean": float(df["n"].mean()) if len(df) else None,
                "max": float(df["n"].max()) if len(df) else None,
                "min": float(df["n"].min()) if len(df) else None
            }
            time.sleep(0.5)
            return {"summary": summary}
        else:
            time.sleep(1)
            return {"message": f"Processed task '{task.id}' (id {task.id})"}
    
    def stop(self):
        self._stop_event.set() # Signal the worker thread to stop.
        self.executor.shutdown(wait = True) # Shutdown the thread pool executor, waiting for all tasks to complete.