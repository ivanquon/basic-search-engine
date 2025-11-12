import nltk
import pathlib
import json
from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer

source_folder = "sources/ANALYST"

stemmer = PorterStemmer()

def tokenize(string: str) -> set[str]:
    tokens = set()
    cleaned = ''.join(char.lower() if char.isalnum() and char.isascii() else ' ' for char in string) 
    words = cleaned.split()
    tokens.update(words)
    return tokens

def index_file(source_folder):
    index = dict()
    p = pathlib.Path(source_folder)
    paths = p.rglob("*.json")
    docID = 0
    for path in paths:
        with open(path, "r") as file:
            data = json.loads(file.readline())
            print(data["url"])
            soup = BeautifulSoup(data["content"], "lxml", from_encoding=data["encoding"])
            tokens = tokenize(soup.get_text())
            stems = {stemmer.stem(token) for token in tokens}
            for stem in stems:
                if index.get(stem):
                    index[stem].append(docID)
                else:
                    index[stem] = [docID,]
        docID += 1
        # if docID > 3:
        #     break
    # print(index)
    with open("index.json", "w") as f:
        json.dump(index, f)

if __name__ == "__main__":
    index_file(source_folder)