from nltk.stem import PorterStemmer
from indexer import tokenize
import datetime
from indexer import Posting, posting_decoder
import json

stemmer = PorterStemmer()

#fixed = "masters of software engineering"
#"ACM"
#Store shortest posting as a base
#Intersect each posting list after

def boolean_and(postings: list[list[Posting]]):
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

if __name__ == "__main__":
    with open("fastindex.json", "r") as fast:
        fastindex = json.load(fast)
    with open("urlmap.json", "r") as urlmap:
        urls = json.load(urlmap)
    while True:
        query = input("Enter your query: ")
        if not query:
            continue
        start = datetime.datetime.now()
        unigram = len(query) == 1
        with open("merged_indexes.json", "r") as index:
            unigram_postings = []
            bigram_postings = []
            unigram_processed_query = [stemmer.stem(token) for token in tokenize(query)]
            bigram_processed_query = [f"{stemmer.stem(s1)} {stemmer.stem(s2)}" for tokenList in [tokenize(query)] for s1, s2 in zip(tokenList, tokenList[1:])]

            for token in unigram_processed_query:
                index.seek(fastindex[token])
                unigram_postings.append(json.loads(index.readline(), object_hook=posting_decoder)[token])
            unigram_ranked = sorted([posting for posting in boolean_and(unigram_postings)], key=lambda p: p.tfidf, reverse=True)

            for token in bigram_processed_query:
                index.seek(fastindex[token])
                bigram_postings.append(json.loads(index.readline(), object_hook=posting_decoder)[token])
            bigram_ranked = sorted([posting for posting in boolean_and(bigram_postings)], key=lambda p: p.tfidf, reverse=True)

            unigram_top_five = [(urls[str(posting.docid)], posting.tfidf) for posting in unigram_ranked][:5]
            bigram_top_five = [(urls[str(posting.docid)], posting.tfidf) for posting in bigram_ranked][:5]

            print("UNIGRAM RESULTS")
            for result in unigram_top_five:
                print(result[0], "with a score of", result[1])
            print("BIGRAM RESULTS")
            for result in bigram_top_five:
                print(result[0], "with a score of", result[1])


        end = datetime.datetime.now()
        total = end - start
        print("Query finished in", total.microseconds/1000, "ms")