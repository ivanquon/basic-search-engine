import pathlib
import json
import re
import utils.logger
import heapq
import shutil
import os
import orjson
import hashlib
import warnings
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from nltk.stem import PorterStemmer
from contextlib import ExitStack
from math import log

source_folder = "sources/DEV"

stemmer = PorterStemmer()

# importance_values = {"title": 5, "h1": 2, "h2": 0.6, "h3": 0.3, "strong": 0.1}
importance_values = {"title": 16, "h1": 8, "h2": 4, "h3": 2, "strong": 1}

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
    

def posting_decoder(obj_dict: dict):
    if "docid" in obj_dict.keys():
        return Posting(obj_dict["docid"], obj_dict["tfidf"], obj_dict["fields"])
    return obj_dict

def stem_tokenize(content: str) -> list[str]:
    return [stemmer.stem(match.group(0)) for match in re.finditer(r"[a-zA-Z0-9]+", content)]

def offload_index(index: dict, docs: int) -> None:
    index_path = pathlib.Path(f"indexes/index{docs}.json")
    index_path.parent.mkdir(exist_ok=True, parents=True)
    with index_path.open("w") as file:
        for item in sorted(index.items()):
            file.write(json.dumps(item) + "\n")

def merge_indexes(numDocs: int) -> None:
    fastindex = dict()
    indexes_path = pathlib.Path("indexes")
    paths = list(indexes_path.rglob("*.json"))
    with ExitStack() as stack:
        with open("merged_indexes.json", "w") as out:
            files = [stack.enter_context(open(path)) for path in paths]
            lines = {file: file.readline() for file in files}
            while lines.values():
                min_term = orjson.loads(min(lines.values()))[0]
                to_merge = {file: json.loads(lines[file], object_hook=posting_decoder)[1] for file in lines if orjson.loads(lines[file])[0]==min_term}
                merged = list(heapq.merge(*to_merge.values()))
                fastindex[min_term] = out.tell()
                out.write(json.dumps({min_term: [Posting(item.docid, (1+log(item.tfidf, 10))*log(numDocs/len(merged), 10) + item.fields, item.fields).__dict__ for item in merged]}) + "\n")
                for f in to_merge.keys():
                    lines[f] = f.readline()
                    if lines[f] == "":
                        del lines[f]
    with open("fastindex.json", "w") as file:
        json.dump(fastindex, file, indent=4)

def index_file(source_folder: str):
    index = dict()
    urlmap = dict()
    dupes = set()
    p = pathlib.Path(source_folder)
    paths = p.rglob("*.json")
    docID = 0
    logger.info("INDEXING STARTED")
    for path in paths:
        with path.open("r") as file:
            data = orjson.loads(file.readline())
            print(data["url"])
            checkhash=hash(data["content"])
            if checkhash not in dupes:
                dupes.add(checkhash)
                urlmap[docID] = data["url"]
                soup = BeautifulSoup(data["content"], 'lxml')
                unigram_tokens = stem_tokenize(soup.get_text(separator=" ", strip=True))
                bigram_tokens = (f"{t1} {t2}" for t1, t2 in zip(unigram_tokens, unigram_tokens[1:]))
                important_tags = ((elem.name, stem_tokenize(elem.get_text(separator=" ", strip=True))) for elem in soup.find_all(["title", "h1", "h2", "h3", "strong"]))
                unigram_important = ((important[0], token) for important in important_tags for token in important[1])
                bigram_important = ((important[0], f"{s1} {s2}") for important in important_tags for s1, s2 in zip(important[1], important[1][1:]))

                for tokenList in [unigram_tokens, bigram_tokens]:
                    for token in tokenList:
                        if token not in index:
                            index[token] = []
                        if index[token] and index[token][-1]["docid"] == docID:
                            index[token][-1]["tfidf"] += 1
                        else:
                            index[token].append({"docid": docID, "tfidf": 1, "fields": 0})
                for importantList in [unigram_important, bigram_important]:
                    for level, token in importantList:
                        index[token][-1]["fields"] += importance_values[level]
                docID += 1
                if docID%10000 == 0:
                    logger.info(f"{docID} FINISHED")
                    offload_index(index, docID)
                    index.clear()
    offload_index(index, docID)
    with open("urlmap.json", "w") as file:
        json.dump(urlmap, file, indent=4)
    logger.info("INDEXING FINISHED")
    logger.info("MERGING STARTED")
    merge_indexes(docID)
    logger.info("MERGING FINISHED")

if __name__ == "__main__":
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
    try: #Clean up past partial indexes before reindexing
        if os.path.isdir("indexes"):
            shutil.rmtree("indexes")
    except OSError as e:
        raise e
    index_file(source_folder)