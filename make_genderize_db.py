import os
import csv
import sqlite3

class GenderDB(object):
    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = sqlite3.connect(db_file)
        self.conn.row_factory = sqlite3.Row
        self.c = self.conn.cursor()
        # Check if we need to create tables:
        r = self.c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='genders'").fetchone()
        if r is None:
            self.c.execute('''CREATE TABLE IF NOT EXISTS genders (id INTEGER PRIMARY KEY,
                                                name TEXT,
                                                male INTEGER,
                                                female INTEGER,
                                                country_id TEXT,
                                                language_id TEXT
                                                )''')                                 
            self.conn.commit()
    def insert(self, row):
        self.c.execute('INSERT INTO genders (id, name, male, female, country_id, language_id) VALUES (?, ?, ?, ?, ?, ?)', 
                       (row[0], row[1], row[2], row[3], row[4], row[5]))
        self.conn.commit()
    def close(self):
        self.conn.close()        
        
def main():
    os.chdir("/Users/gjm/Documents/_Works in Progress/Regulations/data")
    mydb = GenderDB("genders.sqlite")
    
    f = open("genderize.csv", 'r', encoding='utf-8')
    r = csv.reader(f)
    # Skip the header row    
    next(r)
    while r:
        row = next(r)
        mydb.insert(row)
    f.close()
    mydb.close()
    
if __name__ == "__main__":
    main()