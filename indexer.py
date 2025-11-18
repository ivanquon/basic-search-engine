import pathlib
import json
import pickle
from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
import re
import utils.logger

source_folder = "sources/ANALYST"

stemmer = PorterStemmer()

logger = utils.logger.get_logger("INDEXER")

class Posting:
    def __init__(self, docid, tfidf, fields = None):
        self.docid = docid
        self.tfidf = tfidf # use freq counts for now
        self.fields = fields

    def __eq__(self, other):
        return self.docid==other.docid

    def __hash__(self):
        return hash(self.docid)
    
def tokenize(content: str) -> list[str]:
    return re.findall(r"[a-zA-Z0-9]+", content)

def offload_index(index: dict, docs: int) -> None:
    index_path = pathlib.Path(f"indexes/index{docs}.pkl")
    index_path.parent.mkdir(exist_ok=True, parents=True)
    with index_path.open("wb") as file:
        pickle.dump(index, file)

def merge_indexes() -> None:
    indexes_path = pathlib.Path("indexes")
    paths = list(indexes_path.rglob("*.pkl"))
    with open("merged_indexes.pkl", "wb") as file:
        pickle.dump(merge_helper(paths), file)

def merge_helper(paths: list[pathlib.Path]) -> dict:
    if len(paths) == 1:
        with paths[0].open("rb") as file:
            return pickle.load(file)
    else:
        first_half = merge_helper(paths[:len(paths)//2])
        second_half = merge_helper(paths[len(paths)//2:])
        return {key : sorted(first_half.get(key,[]) + second_half.get(key,[]), key=lambda posting: posting.docid) for key in first_half.keys() | second_half.keys()}

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

                if index[stemmed] and index[stemmed][-1].docid == docID:
                    index[stemmed][-1].tfidf += 1
                else:
                    index[stemmed].append(Posting(docID, 1))

        docID += 1
        if docID%200 == 0:
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