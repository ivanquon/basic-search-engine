from typing import Union

from fastapi import FastAPI
from retrieval import retrieve
import json

app = FastAPI()

with open("fastindex.json", "r") as fast:
    fastindex = json.load(fast)
with open("urlmap.json", "r") as urlmap:
    urls = json.load(urlmap)

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/retrieval/{query}")
def search_query(query: str):
    results = retrieve(query, fastindex, urls)
    return {"urls": results[0], "mstime": results[1]}