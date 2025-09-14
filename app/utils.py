from .manager import TaskManager
import pandas as pd 

def tasks_df(manager: TaskManager) -> pd.DataFrame:
    tasks = manager.list_tasks()
    rows = []
    for t in tasks:
        rows.append({
            'id': t.id,
            'title': t.title,
            'status': t.status,
            'payload': t.payload,
            'result': t.result,
            'created_at': t.created_at,
            'priority': t.priority
        })
    df = pd.DataFrame(rows)
    return df

def summary_json(manager: TaskManager):
    df = tasks_df(manager)
    resp = {}
    resp['total_tasks'] = int(len(df))
    resp['status_counts'] = df['status'].value_counts().to_dict() if not df.empty else {}

    def extract_sum(r):
        try:
            return r.get("summary", {}).get("sum")
        except:
            return None 
    if not df.empty:
        df['result_sum'] = df['result'].apply(lambda r: extract_sum(r) if r else None)
        numeric_sums = df['result_sum'].dropna().astype(float)
        resp['result_sum_stats'] = {
            'count': int(numeric_sums.count()),
            'total': float(numeric_sums.sum()) if len(numeric_sums) else 0.0,
            'mean': float(numeric_sums.mean()) if len(numeric_sums) else None
        }
    else:
        resp['result_sum_stats'] = {}
    return resp