from nltk.stem import PorterStemmer
from indexer import stem_tokenize
import datetime
from indexer import Posting, posting_decoder
import json


stemmer = PorterStemmer()

test_queries = [
    #Base Queries
    "ACM", 
    "master of software engineering", 
    "cristina lopes", 
    "machine learning", 
    #Difficult Queries with a lot of postings
    "to be or not to be", 
    "and so on and so forth",
    #Names likely found in citations/self page
    "Michael Shindler", 
    "ray klefstad",
    "Cristina Videira Lopes",
    #General UCI Sites
    "Donald Bren School of Information and Computer Sciences",
    #Partial match handling
    "Quarterly Academic Calendar",
    "UCI Police Department",
    "a the heart",
    #Exact match stress testing
    "Supercalifragilisticexpialidocious",
    "Software is everywhere, and every day it becomes a more integrated and indispensable part of how we live. Understanding its inner workings, as well as the complex systems it makes possible, is critical to ongoing technological innovation. By both delving deep into the technical underpinnings of existing systems and continuously experimenting with new software tools and platforms, Professor Cristina Lopes is pioneering the development of innovative, global-scale and immersive environments that help people lead better, fuller and richer lives.",
    "Introduction to ubiquitous computing research methods, tools and techniques. Prototyping, design and evaluation of physical computing applications, smart environments, embedded systems and future computing scenarios. Includes hands-on in-class laboratory exercises.",
    #Relevancy of common searches
    "Undergraduate Academic Advising",
    "university registrar",
    "Principles of Data Management",
    "Fall 2018 Photos",
    "Video Game Development Club",
    "Critical Writing on Information Technology",
    "Project in Ubiquitous Computing",
    #No Matches
    "dwajdiowajoisdjowa",
    ]

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

def retrieve(query: str, fastindex, urls):
    start = datetime.datetime.now()
    with open("merged_indexes.json", "r") as index:
        postings = []
        stem_tokenized_query = stem_tokenize(query)
        unigram = len(stem_tokenized_query) == 1
        precessed_query = stem_tokenized_query if unigram else [f"{s1} {s2}" for s1, s2 in zip(stem_tokenized_query, stem_tokenized_query[1:])]

        for token in precessed_query:
            #Return as many matches as possible (if full match not available do partial)
            try:
                index.seek(fastindex[token])
                postings.append(json.loads(index.readline(), object_hook=posting_decoder)[token])
            except KeyError:
                continue
        ranked = sorted([posting for posting in boolean_and(postings)], key=lambda p: p.tfidf, reverse=True)

        top_five = [(urls[str(posting.docid)], posting.tfidf) for posting in ranked][:5]

    end = datetime.datetime.now()
    total = end - start
    return [top_five[:5], total.microseconds/1000]

if __name__ == "__main__":
    with open("fastindex.json", "r") as fast:
        fastindex = json.load(fast)
    with open("urlmap.json", "r") as urlmap:
        urls = json.load(urlmap)
    for query in test_queries:
        results = retrieve(query, fastindex, urls)
        print("RESULTS")
        for result in results[0]:
            print(result[0], "with a score of", result[1])
        print("Query finished in", results[1], "ms")
    while True:
        query = input("Enter your query: ")
        if not query:
            continue
        results = retrieve(query, fastindex, urls)
        print("RESULTS")
        for result in results[0]:
            print(result[0], "with a score of", result[1])
        print("Query finished in", results[1], "ms")