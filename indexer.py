import pathlib
import json
from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
import re
import utils.logger
from contextlib import ExitStack
import heapq
from math import log

source_folder = "sources/DEV"

stemmer = PorterStemmer()

importance_values = {"title": 5, "h1": 2, "h2": 0.6, "h3": 0.3, "strong": 0.1}

logger = utils.logger.get_logger("INDEXER")

class Posting:
    def __init__(self, docid, tfidf, fields):
        self.docid = docid
        self.tfidf = tfidf # use freq counts for now
        self.fields = fields

    def __eq__(self, other):
        return self.docid == other.docid

    def __lt__(self, other):
        return self.docid < other.docid

    def __hash__(self):
        return hash(self.docid)
    
    def __repr__(self):
        return f"Posting({self.docid}, {self.tfidf}, {self.fields})"
    

def posting_decoder(obj_dict):
    if "docid" in obj_dict.keys():
        return Posting(obj_dict["docid"], obj_dict["tfidf"], obj_dict["fields"])
    return obj_dict
    
def tokenize(content: str) -> list[str]:
    return re.findall(r"[a-zA-Z0-9]+", content)

def offload_index(index: dict, docs: int) -> None:
    index_path = pathlib.Path(f"indexes/index{docs}.json")
    index_path.parent.mkdir(exist_ok=True, parents=True)
    with index_path.open("w") as file:
        for item in sorted(index.items()):
            file.write(json.dumps(item) + "\n")

def merge_indexes(numDocs) -> None:
    fastindex = dict()
    indexes_path = pathlib.Path("indexes")
    paths = list(indexes_path.rglob("*.json"))
    with ExitStack() as stack:
        with open("merged_indexes.json", "w") as out:
            files = [stack.enter_context(open(path)) for path in paths]
            lines = {file: file.readline() for file in files}
            while lines.values():
                min_term = json.loads(min(lines.values()))[0]
                to_merge = {file: json.loads(lines[file], object_hook=posting_decoder)[1] for file in lines if json.loads(lines[file])[0]==min_term}
                merged = list(heapq.merge(*to_merge.values()))
                fastindex[min_term] = out.tell()
                out.write(json.dumps({min_term: [Posting(item.docid, (1+log(item.tfidf, 10))*log(numDocs/len(merged), 10) + item.fields, item.fields).__dict__ for item in merged]}) + "\n")
                for f in to_merge.keys():
                    lines[f] = f.readline()
                    if lines[f] == "":
                        del lines[f]
    with open("fastindex.json", "w") as file:
        json.dump(fastindex, file, indent=4)

def index_file(source_folder):
    index = dict()
    urlmap = dict()
    p = pathlib.Path(source_folder)
    paths = p.rglob("*.json")
    docID = 0
    logger.info("INDEXING STARTED")
    for path in paths:
        with path.open("r") as file:
            data = json.load(file)
            print(data["url"])
            urlmap[docID] = data["url"]
            soup = BeautifulSoup(data["content"], from_encoding=data["encoding"])
            tokens = tokenize(soup.get_text())
            important = {(elem.name, stem) for elem in soup.find_all(["title", "h1", "h2", "h3", "strong"]) for stem in tokenize(elem.get_text())}
            for token in tokens:
                stemmed = stemmer.stem(token.lower())
                if stemmed not in index:
                    index[stemmed] = []
                if index[stemmed] and index[stemmed][-1]["docid"] == docID:
                    index[stemmed][-1]["tfidf"] += 1
                else:
                    index[stemmed].append({"docid": docID, "tfidf": 1, "fields": 0})
                
                for level in importance_values:
                    if (level, stemmed) in important:
                        index[stemmed][-1]["fields"] += importance_values[level]
                        important.remove((level, stemmed))
        docID += 1
        if docID%10000 == 0:
            offload_index(index, docID)
            index=dict()
    offload_index(index, docID)
    with open("urlmap.json", "w") as file:
        json.dump(urlmap, file, indent=4)
    logger.info("INDEXING FINISHED")
    logger.info("MERGING STARTED")
    merge_indexes(docID)
    logger.info("MERGING FINISHED")

if __name__ == "__main__":
    index_file(source_folder)