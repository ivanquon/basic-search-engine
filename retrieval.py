from nltk.stem import PorterStemmer
from indexer import tokenize
import datetime
import pickle
from indexer import Posting

stemmer = PorterStemmer()

fixed = "cristina lopes"
#Store shortest posting as a base
#Intersect each posting list after

def boolean_and(postings: list[list[Posting]]):
    common_objects = list(set(postings[0]).intersection(*postings[1:]))
    return common_objects

if __name__ == "__main__":
    #query = input("Enter your query: ")
    query = fixed
    with open("merged_indexes.pkl", "rb") as file:
        index = pickle.load(file)
    start = datetime.datetime.now()

    processed_query = [stemmer.stem(token.lower()) for token in tokenize(query)]
    postings = [index[token] for token in processed_query]
    common = boolean_and(postings)
    print(sorted([posting.docid for posting in common]))


    end = datetime.datetime.now()
    total = end - start
    print(processed_query)
    print(total.microseconds/1000)