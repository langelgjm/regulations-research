import os
import sys
import sqlite3
import unicodedata
import csv
import ucsv
from Common import yield_sql_results, get_sqlite_conn, get_sql_rows, sample_sql_rows,\
    html_entity_translation_table, make_config_dict
from _sqlite3 import Row
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier
from sklearn.pipeline import Pipeline
from sklearn import cross_validation
import numpy as np
import string
import ConfigParser

config_file = "regulations.conf"
config = ConfigParser.ConfigParser()
config.read(config_file)
myconfig = make_config_dict(config)

working_directory = myconfig['general']['working_directory']
training_file = myconfig['nn']['nn_training_file']
test_file = myconfig['nn']['nn_test_file']
db_file = myconfig['nn']['nn_db_file']
nn_class_file = myconfig['nn']['nn_class_file']
conn = None

delchars = ''.join(c for c in map(chr, range(256)) if not c.isalnum())
mymap = string.maketrans(delchars, ''.join(' ' for i in range(194)))
tbl = dict.fromkeys(i for i in xrange(sys.maxunicode) if unicodedata.category(unichr(i)).startswith('P'))
non_alphanum_translation_table = dict.fromkeys(map(ord, delchars), u" ")
s_non_alphanum_translation_table = dict.fromkeys(map(ord, delchars), " ")

###############################################################################

def read_nn_coded_csv(filename):
    '''
    Return a list of dictionaries, where each dictionary represents a row from the CSV file.
    '''
    f = open(filename, 'r')
    mycsv = csv.reader(f)
    docs=[]
    for row in mycsv:
        d = {}
        d["id"] = row[0]
        d["code"] = row[1]
        d["subject"] = row[2]
        d["comment"] = row[3].decode('latin1')
        docs.append(d)
    return docs

def read_nn_coded_ucsv(filename):
    '''
    Return a list of dictionaries, where each dictionary represents a row from the CSV file.
    '''
    f = open(filename, 'r')
    mycsv = ucsv.UnicodeReader(f)
    docs=[]
    try:
        for i, row in enumerate(mycsv):
            d = {}
            d["id"] = row[0]
            d["code"] = row[1]
            d["subject"] = row[2]
            d["comment"] = row[3]
            docs.append(d)
    except UnicodeDecodeError as e:
        print e
        print i
    return docs

def process_str(s):
    for k, v in html_entity_translation_table.iteritems():
        s = s.replace(k, v)
    # Get rid of other punctuation
    s = s.translate(mymap)
    return s

def process_ustr(u):
# Now clean the text columns of the SQL results
    # Strip HTML entities
    for k, v in html_entity_translation_table.iteritems():
        u = u.replace(k, v)
    # Get rid of other punctuation
    u = u.translate(non_alphanum_translation_table)
    # Get rid of Unicode punctuation
    u = u.translate(tbl)
    return u

###############################################################################

def main():
    # Read and clean a training and test data set
    docs = read_nn_coded_csv(training_file)
    for i, d in enumerate(docs):
        d =  process_ustr(d['comment'])
        docs[i]['comment'] = d
    mydata = [d['comment'] for d in docs]
    myclasses = [d['code'] for d in docs]
    
    testdocs = read_nn_coded_csv(test_file)
    for d in testdocs:
        d =  process_ustr(d['comment'])
        testdocs[i]['comment'] = d
    mytestdata = [d['comment'] for d in testdocs]
    mytestclasses = [d['code'] for d in testdocs]
    
    # Now try a naive bayes classifier
    # Has stop words and tokenization built in, but does not have stemming
    text_clf = Pipeline([('cv', CountVectorizer(stop_words="english")),
                         ('tf', TfidfTransformer(use_idf=False)),
                         ('clf', MultinomialNB())])
    text_clf = text_clf.fit(mydata, myclasses)
    predicted = text_clf.predict(mydata)
    # What proportion of predicted classes were the same as the actual classes in the training set?
    np.mean(predicted == np.array(myclasses))
    
    # Do the same thing again, but with a SVM classifier
    text_clf = Pipeline([('cv', CountVectorizer(stop_words="english")),
                         ('tf', TfidfTransformer(use_idf=False)),
                         ('clf', SGDClassifier(loss='hinge', penalty='l2', alpha=1e-3, n_iter=5))])
    text_clf = text_clf.fit(mydata, myclasses)
    predicted = text_clf.predict(mydata)
    
    # What proportion of predicted classes were the same as the actual classes in the training set?
    np.mean(predicted == np.array(myclasses))
    mytestdata = [d['comment'] for d in testdocs]
    mytestclasses = [d['code'] for d in testdocs]
    predicted2 = text_clf.predict(mytestdata)
    np.mean(predicted2 == np.array(mytestclasses))
    
    # Now, let's fit a new model on the combined training and test sets, holding back a portion by using cross-validation to check the performance
    mydata.extend(mytestdata)
    myclasses.extend(mytestclasses)
    
    text_clf = Pipeline([('cv', CountVectorizer(stop_words="english")),
                         ('tf', TfidfTransformer(use_idf=False)),
                         ('clf', MultinomialNB())])
    cv_scores = cross_validation.cross_val_score(text_clf, mydata, myclasses, cv=cross_validation.ShuffleSplit(len(mydata),n_iter=10, train_size=0.9))
    np.mean(cv_scores)
    
    text_clf = Pipeline([('cv', CountVectorizer(stop_words="english")),
                         ('tf', TfidfTransformer(use_idf=False)),
                         ('clf', SGDClassifier(loss='hinge', penalty='l2', alpha=1e-3, n_iter=5))])
    
    cv_scores = cross_validation.cross_val_score(text_clf, mydata, myclasses, cv=cross_validation.ShuffleSplit(len(mydata),n_iter=10, train_size=0.9))
    np.mean(cv_scores)

    # The SVM classifier has slightly better accuracy, so that's what we'll use    
    text_clf = text_clf.fit(mydata, myclasses)
    
    # Now we need to read data from the DB, classify it, and write out the classifications
    conn = get_sqlite_conn(os.path.join(working_directory, db_file))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    # Select the ids and rows of all comments that were not manually coded
    # Write them to a CSV file
    c = c.execute("SELECT id,text FROM fcc_nn WHERE id NOT IN (SELECT id FROM manual_code WHERE random_set < 3)")
    f = open(nn_class_file, 'w')
    for r in yield_sql_results(c, fetch_num=10000):
        comment = process_ustr(dict(r)['text'])
        classification = text_clf.predict([comment])
        s = dict(r)['id'] + "," + classification[0] + "\n"
        f.write(s)
    f.close()
    conn.close()
    
if __name__ == "__main__":
    main()
