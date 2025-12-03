from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from retrieval import retrieve
import json

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # or ["http://localhost:3000"]
    allow_methods=["*"],
    allow_headers=["*"],
)


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