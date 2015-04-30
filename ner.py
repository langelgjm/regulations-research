import os
os.chdir("/Users/gjm/Documents/_Works in Progress/Regulations/regulations/")
import configparser
from Common import make_config_dict, get_sqlite_conn
import nltk

def extract_entities(text):
    for sent in nltk.sent_tokenize(text):
        for chunk in nltk.ne_chunk(nltk.pos_tag(nltk.word_tokenize(sent))):
            if type(chunk) is nltk.tree.Tree:
                print(chunk)

def extract_persons(text):
    for sent in nltk.sent_tokenize(text):
        for chunk in nltk.ne_chunk(nltk.pos_tag(nltk.word_tokenize(sent))):
            if type(chunk) is nltk.tree.Tree:
                if chunk.label() == "PERSON":
                    print(chunk)                

def main():   
    config_file = "regulations.conf"
    config = configparser.ConfigParser()
    config.read(config_file)
    myconfig = make_config_dict(config)

    db_file = myconfig['hhs']['db_file']
    conn = get_sqlite_conn(db_file)
    rs = conn.execute('SELECT documentId, comment FROM documents WHERE docketId == "CMS-2012-0031" LIMIT 1000').fetchall()

    for r in rs:
        nes = extract_persons(r['comment'])
        if nes:
            print(r['documentId'])
            print(nes)

if __name__ == "__main__":
    main()