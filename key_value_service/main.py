from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Key-Value DB Service")

store = {}

class KeyValue(BaseModel):
    key: str
    value: str

@app.post("/set")
def set_value(data: KeyValue):
    store[data.key] = data.value
    return {"status": "success", "key": data.key}

@app.get("/get/{key}")
def get_value(key: str):
    if key not in store:
        raise HTTPException(status_code=404, detail="Key not found")
    return {"key": key, "value": store[key]}

@app.delete("/delete/{key}")
def delete_value(key: str):
    if key not in store:
        raise HTTPException(status_code=404, detail="Key not found")
    del store[key]
    return {"status": "deleted", "key": key}
