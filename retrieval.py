from nltk.stem import PorterStemmer
from indexer import tokenize
import datetime
from indexer import Posting, posting_decoder
import json

stemmer = PorterStemmer()


test_queries = ["ACM", 
                "master of software engineering", 
                "cristina lopes", 
                "machine learning", 
                "to be or not to be", 
                "and so on and so forth",
                "klefstad", 
                "ray klefstad", 
                "Donald Bren School of Information and Computer Sciences", 
                "Quarterly Academic Calendar",
                "Undergraduate Academic Advising ",
                "university registrar",
                "Supercalifragilisticexpialidocious",
                "Software is everywhere, and every day it becomes a more integrated and indispensable part of how we live. Understanding its inner workings, as well as the complex systems it makes possible, is critical to ongoing technological innovation. By both delving deep into the technical underpinnings of existing systems and continuously experimenting with new software tools and platforms, Professor Cristina Lopes is pioneering the development of innovative, global-scale and immersive environments that help people lead better, fuller and richer lives.",
                "a",
                "a the heart"]

#Store shortest posting as a base
#Intersect each posting list after

def boolean_and(postings: list[list[Posting]]) -> list:
    if not postings:
        return []
    by_length = sorted(postings, key = lambda p: len(p))
    common = by_length[0]
    for postings_list in by_length[1:]:
        new_common = []
        i = j = 0
        while i < len(common) and j < len(postings_list):
            if common[i] == postings_list[j]:
                new_common.append(Posting(common[i].docid, common[i].tfidf + postings_list[j].tfidf, common[i].fields))
                i+=1
                j+=1
            elif common[i] < postings_list[j]:
                i+=1
            else:
                j+=1
        common = new_common
    return common

def retrieve(query: str):
    start = datetime.datetime.now()
    with open("merged_indexes.json", "r") as index:
        postings = []
        tokenized_query = tokenize(query)
        unigram = len(tokenized_query) == 1
        precessed_query = [stemmer.stem(token) for token in tokenize(query)] if unigram else [f"{stemmer.stem(s1)} {stemmer.stem(s2)}" for tokenList in [tokenize(query)] for s1, s2 in zip(tokenList, tokenList[1:])]

        for token in precessed_query:
            try:
                index.seek(fastindex[token])
                postings.append(json.loads(index.readline(), object_hook=posting_decoder)[token])
            except KeyError:
                continue
        ranked = sorted([posting for posting in boolean_and(postings)], key=lambda p: p.tfidf, reverse=True)

        top_five = [(urls[str(posting.docid)], posting.tfidf) for posting in ranked][:5]

        print("RESULTS")
        for result in top_five:
            print(result[0], "with a score of", result[1])

    end = datetime.datetime.now()
    total = end - start
    print("Query finished in", total.microseconds/1000, "ms")

if __name__ == "__main__":
    with open("fastindex.json", "r") as fast:
        fastindex = json.load(fast)
    with open("urlmap.json", "r") as urlmap:
        urls = json.load(urlmap)
    while True:
        query = input("Enter your query: ")
        if not query:
            continue
        retrieve(query)