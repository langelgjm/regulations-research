import numpy as np
import pandas as pd
import nltk
import re
import os
os.chdir("/Users/gjm/Documents/_Works in Progress/Regulations/regulations/")
import configparser
from Common import make_config_dict, get_sqlite_conn
from sklearn import feature_extraction
from sklearn.metrics.pairwise import cosine_similarity
from scipy.cluster.hierarchy import ward, dendrogram, to_tree, leaves_list
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import matplotlib as mpl

config_file = "regulations.conf"
config = configparser.ConfigParser()
config.read(config_file)
myconfig = make_config_dict(config)

db_file = myconfig['hhs']['db_file']
conn = get_sqlite_conn(db_file)


# load nltk's English stopwords as variable called 'stopwords'
stopwords = nltk.corpus.stopwords.words('english')
# load nltk's SnowballStemmer as variabled 'stemmer'
from nltk.stem.snowball import SnowballStemmer
stemmer = SnowballStemmer("english")

class reg_doc(object):
    def __init__(self, row):
        '''
        Pass this SQLite rows from SELECT queries on documents, attachments, authors, entities
        '''
        for k in row.keys():
            self.__setattr__(k, row[k])
        self.tokenized = False
        self.stemmed = False
    def __len__(self):
        '''
        Return character length of tweet text. If no tweet data, returns None.
        '''
        try:
            return len(self.comment)
        except AttributeError:
            return None
    def __repr__(self):
        return repr(self.__dict__)
    def __nonzero__(self):
        return True
    def keys(self):
        return self.__dict__.keys()
    def tokenize(self):
        # first tokenize by sentence, then by word to ensure that punctuation is caught as it's own token
        tokens = [word.lower() for sent in nltk.sent_tokenize(self.comment) for word in nltk.word_tokenize(sent)]
        self.tokens = []
        # filter out any tokens not containing letters (e.g., numeric tokens, raw punctuation)
        for token in tokens:
            if re.search('[a-zA-Z]', token):
                self.tokens.append(token)
        self.tokenized = True
        return self.tokens 
    def stem(self):
        self.stems = []
        if self.tokenized:
            self.stems = [stemmer.stem(t) for t in self.tokens]
        self.stemmed = True
        return self.stems
    def toString(self, l):
        return ' '.join(l)


def maxTreeDist(root_node):
    if not node.is_leaf:
        


def get_docs(conn, query):
    rs = conn.execute(query)
    return [reg_doc(r) for r in rs]

query = '''SELECT * FROM documents WHERE docketId == "CMS-2012-0031" LIMIT 1000'''
docs = get_docs(conn, query)

for doc in docs:
    doc.tokenize()
    doc.stem()

count_vect = feature_extraction.text.CountVectorizer(stop_words="english")
term_freq = feature_extraction.text.TfidfTransformer(use_idf=True)

X = count_vect.fit_transform([doc.toString(doc.stems) for doc in docs])
X = term_freq.fit_transform(X)

dist = 1 - cosine_similarity(X)
linkage_matrix = ward(dist)

tree = to_tree(linkage_matrix, rd=True)

#fig, ax = plt.subplots(figsize=(15, 20)) # set size
R = dendrogram(linkage_matrix, orientation="right", distance_sort = "descending", labels=[doc.documentId for doc in docs]);
#
#plt.tick_params(\
#    axis= 'x',          # changes apply to the x-axis
#    which='both',      # both major and minor ticks are affected
#    bottom='off',      # ticks along the bottom edge are off
#    top='off',         # ticks along the top edge are off
#    labelbottom='off')
#
#plt.tight_layout()