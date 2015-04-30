import os
os.chdir("/Users/gjm/Documents/_Works in Progress/Regulations/regulations")
import sqlite3
from Common import get_sqlite_conn, make_config_dict
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
#from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier
from sklearn.pipeline import Pipeline
from sklearn import cross_validation
import numpy as np
import configparser
import pandas

config_file = "regulations.conf"
config = configparser.ConfigParser()
config.read(config_file)
myconfig = make_config_dict(config)

docket = "hhs"
#working_directory = myconfig['general']['working_directory']
#training_file = myconfig['nn']['nn_training_file']
#test_file = myconfig['nn']['nn_test_file']
db_file = myconfig[docket]['db_file']
#nn_class_file = myconfig['nn']['nn_class_file']
conn = None

###############################################################################

def get_docs(cur, doc_ids):
    '''
    Return list of text associated with the input doc_ids from the db.
    '''
    doc_list = []
    for doc_id in doc_ids:
        doc = cur.execute('''SELECT comment FROM documents WHERE documentId = ?''', (doc_id['id'], )).fetchone()
        doc_list.append(doc['comment'])
    return doc_list

def get_doc_classes(cur, doc_ids):
    '''
    Return list of classes assoicated with the input doc_ids from the db.
    '''
    classes_list = []
    for doc_id in doc_ids:
        code = cur.execute('''SELECT code FROM coded_temp WHERE id = ?''', (doc_id['id'], )).fetchone()
        classes_list.append(code['code'])
    return classes_list

###############################################################################

def main():
    conn = get_sqlite_conn(db_file)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    # Get training data doc_ids
    doc_ids = c.execute('''SELECT id FROM coded_temp WHERE id IN (SELECT documentId FROM documents WHERE docketId = ?)''', (myconfig[docket]['docketid'], )).fetchall()
    docs = get_docs(c, doc_ids)
    classes = get_doc_classes(c, doc_ids)
    
    # Take a look at the prior distribution:
    s = pandas.Series(classes)
    pandas.crosstab(s, ['s', 'o'] )

#    # Now try a naive bayes classifier
#    # Has stop words and tokenization built in, but does not have stemming
#    mypl = Pipeline([('cv', CountVectorizer(stop_words="english")),
#                         ('tf', TfidfTransformer(use_idf=False)),
#                         ('clf', MultinomialNB())])
#    myfit = mypl.fit(docs, classes)
#    predicted = myfit.predict(docs)
#    # What proportion of predicted classes were the same as the actual classes in the training set?
#    np.mean(predicted == np.array(classes))
#    
#    # Do the same thing again, but with an SGD classifier
#    mypl = Pipeline([('cv', CountVectorizer(stop_words="english")),
#                         ('tf', TfidfTransformer(use_idf=False)),
#                         ('clf', SGDClassifier(loss='hinge', penalty='l2', alpha=1e-3, n_iter=5))])
#    myfit = mypl.fit(docs, classes)
#    predicted = myfit.predict(docs)
#    # What proportion of predicted classes were the same as the actual classes in the training set?
#    np.mean(predicted == np.array(classes))
#    
#    # Cross validation
#    mypl = Pipeline([('cv', CountVectorizer(stop_words="english")),
#                         ('tf', TfidfTransformer(use_idf=False)),
#                         ('clf', MultinomialNB())])
#    cv_scores = cross_validation.cross_val_score(mypl, docs, classes, cv=cross_validation.ShuffleSplit(len(docs),n_iter=10, train_size=0.9))
#    np.mean(cv_scores)
    
    mypl = Pipeline([('cv', CountVectorizer(stop_words="english")),
                         ('tf', TfidfTransformer(use_idf=False)),
                         ('clf', SGDClassifier(loss='hinge', penalty='l2', alpha=1e-3, n_iter=5))])
    cv_scores = cross_validation.cross_val_score(mypl, docs, classes, cv=cross_validation.ShuffleSplit(len(docs),n_iter=10, train_size=0.9))
    np.mean(cv_scores)

    # The SGD classifier has slightly better accuracy, so that's what we'll use    
    myfit = mypl.fit(docs, classes)
    
###############################################################################    
    
    # Select the ids and rows of all comments that were not manually coded
    # Write them to a CSV file
    docs = c.execute("SELECT documentId,comment FROM documents WHERE documentId NOT IN (SELECT id FROM coded_temp) AND docketId = ?", (myconfig[docket]['docketid'], )).fetchall()
#    f = open(myconfig[docket]['class_file'], 'w')
    for doc in docs:
        if doc['comment'] is not None:
            classification = myfit.predict([doc['comment']])
        else:
            classification = ["na"]
        c.execute('''UPDATE DOCUMENTS SET class = ? WHERE documentId = ?''', (classification[0], doc['documentId']))
#        s = doc['documentId'] + "," + classification[0] + "\n"
#        f.write(s)
#    f.close()
    conn.commit()

    docs_coded = c.execute("SELECT id,code FROM coded_temp WHERE id IN (SELECT documentId FROM documents WHERE docketId = ?)", (myconfig[docket]['docketid'], )).fetchall()
    for doc in docs_coded:
        c.execute('''UPDATE DOCUMENTS SET class = ? WHERE documentId = ?''', (doc['code'], doc['id']))
    conn.commit()

    conn.close()
    
if __name__ == "__main__":
    main()
