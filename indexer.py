import pathlib
import json
from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
import re
import utils.logger

source_folder = "sources/DEV"

stemmer = PorterStemmer()

logger = utils.logger.get_logger("INDEXER")

class Posting:
    def __init__(self, docid, tfidf, fields = None):
        self.docid = docid
        self.tfidf = tfidf # use freq counts for now
        self.fields = fields
    
def tokenize(content: str) -> list[str]:
    return re.findall(r"[a-zA-Z0-9]+", content)

def offload_index(index: dict, docs: int) -> None:
    index_path = pathlib.Path(f"indexes/index{docs}.json")
    index_path.parent.mkdir(exist_ok=True, parents=True)
    with index_path.open("w") as file:
        json.dump(index, file, indent=4)

def merge_indexes() -> None:
    indexes_path = pathlib.Path("indexes")
    paths = list(indexes_path.rglob("*.json"))
    with open("merged_indexes.json", "w") as file:
        json.dump(merge_helper(paths), file, indent= 4)

def merge_helper(paths: list[pathlib.Path]) -> dict:
    if len(paths) == 1:
        with paths[0].open("r") as file:
            return json.load(file)
    else:
        first_half = merge_helper(paths[:len(paths)//2])
        second_half = merge_helper(paths[len(paths)//2:])
        return {key : sorted(first_half.get(key,[]) + second_half.get(key,[]), key=lambda posting: posting["docid"]) for key in first_half.keys() | second_half.keys()}

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
            for token in tokens:
                stemmed = stemmer.stem(token.lower())
                if stemmed not in index:
                    index[stemmed] = []

                if index[stemmed] and index[stemmed][-1]["docid"] == docID:
                    index[stemmed][-1]["freq"] += 1
                else:
                    index[stemmed].append({"docid": docID, "freq": 1})

        docID += 1
        if docID%10000 == 0:
            offload_index(index, docID)
            index=dict()
    offload_index(index, docID)
    with open("urlmap.json", "w") as file:
        json.dump(urlmap, file, indent=4)
    logger.info("INDEXING FINISHED")
    logger.info("MERGING STARTED")
    merge_indexes()
    logger.info("MERGING FINISHED")

if __name__ == "__main__":
    index_file(source_folder)