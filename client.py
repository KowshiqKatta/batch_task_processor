import requests
import time

BASE = "http://127.0.0.1:8000/"

def create_task(title, numbers):
    payload = {"title": title, "payload": {"numbers": numbers}}
    r = requests.post(f"{BASE}/tasks", json=payload)
    print("create_task:", r.status_code, r.json())
    return r.json()

def upload_csv_string():
    csv_content = "title,numbers\nBatch One,1;2;3;4\nBatch Two,10;20;30"
    files = {"file": ("tasks.csv", csv_content)}
    r = requests.post(f"{BASE}/upload-csv", files=files)
    print("upload_csv:", r.status_code, r.json())
    return r.json()

def get_report():
    r = requests.get(f"{BASE}/report")
    print("report:", r.status_code, r.json())
    return r.json()

if __name__ == "__main__":
    create_task("quick-sum", [3,4,5])
    upload_csv_string()
    print("sleeping 2s to let worker process...")
    time.sleep(3)
    get_report()
