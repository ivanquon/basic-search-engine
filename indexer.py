import pathlib
import json
from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer
import re

source_folder = "sources/DEV"

stemmer = PorterStemmer()

class Posting:
    def __init__(self, docid, tfidf, fields = None):
        self.docid = docid
        self.tfidf = tfidf # use freq counts for now
        self.fields = fields
    
def tokenize(content: str) -> list[str]:
    # cleaned = ''.join(char.lower() if char.isalnum() and char.isascii() else ' ' for char in content) 
    # words = cleaned.split()
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
    p = pathlib.Path(source_folder)
    paths = p.rglob("*.json")
    docID = 0
    for path in paths:
        with path.open("r") as file:
            data = json.load(file)
            print(data["url"])
            soup = BeautifulSoup(data["content"], "lxml", from_encoding=data["encoding"])
            tokens = tokenize(soup.get_text())
            freqs = {}
            for token in tokens:
                freqs[stemmer.stem(token.lower())] = freqs.get(stemmer.stem(token.lower()), 0) + 1
            # stems = [stemmer.stem(token.lower()) for token in tokens]
            # unique_stems = set(stems)
            for stem in freqs.keys():
                if index.get(stem):
                    index[stem].append(Posting(docID, freqs[stem]).__dict__)
                else:
                    index[stem] = [Posting(docID, freqs[stem]).__dict__,]
        docID += 1
        # if docID > 10:
        #     break
        if docID%1000 == 0:
            offload_index(index, docID)
            index=dict()
    offload_index(index, docID)
    merge_indexes()

if __name__ == "__main__":
    index_file(source_folder)