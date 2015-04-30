import os
import sys
import sqlite3
from Common import yield_sql_results, get_sqlite_conn, make_config_dict
import ConfigParser
import random

config_file = "regulations.conf"
config = ConfigParser.ConfigParser()
config.read(config_file)
myconfig = make_config_dict(config)

working_directory = myconfig['general']['working_directory']
db_file = myconfig['aca']['db_file']
conn = None

def get_random_rowids(c, n=1000):
    all_rowids = []
    for r in yield_sql_results(c):
        (untupled_r, ) = r
        all_rowids.append(untupled_r)
    random_rowids = random.sample(all_rowids, n)
    return random_rowids

def main():
    if len(sys.argv) < 2:
        print "Usage: " + sys.argv[0] + " <CMS-2012-0031 | DOS-2014-0003> <number of randomly selected comments to retrieve>"
        sys.exit()
    docket_arg = sys.argv[1]
    random_set_len = int(sys.argv[2])
    
    conn = get_sqlite_conn(os.path.join(working_directory, db_file))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    # Get all rowids for documents that are not already in the manual_code table
    print "Getting rowids from " + str(docket_arg) + "."
    # note that sqlite syntax requires an AND, not an & (ampersand)
    c = c.execute("SELECT ROWID FROM documents WHERE docketId==? AND documentId NOT IN (SELECT id FROM manual_code)", (docket_arg,))
    #print "Sampling " + str(random_set_len) + " rowids."
    random_rowids = get_random_rowids(c, n=random_set_len)
    
    print "Inserting sampled documents into manual_code table."
    random_set = c.execute("SELECT MAX(random_set) FROM manual_code").fetchone()["MAX(random_set)"]
    if random_set is None:
        random_set = 1
    else:
        random_set += 1
    print "This is random_set " + str(random_set)
    for r in random_rowids:
        print r
        documentId = c.execute("SELECT documentId FROM documents WHERE ROWID==?", (r,)).fetchone()["documentId"]
        print documentId
        c.execute("INSERT INTO manual_code VALUES (?, ?)", (documentId, random_set))
    conn.commit()
    conn.close()
    print "Done."
    
    # Now the idea is to use the manual_code table to export data directly from sqlite as CSV
    
if __name__ == "__main__":
    main()