import sys
import os
import re
import sqlite3
from Common import get_sqlite_conn, get_sql_row_as_dict

db_file = 'hhs_dos_comment_db.sqlite'
log_file = 'hhs_parse.log'
db_dir = "/Users/gjm/Documents/_Works in Progress/Regulations/data/"
data_dir = "/Users/gjm/Documents/_Works in Progress/Regulations/data/hhs/attachments/parse/"

#salutations = ['Ms', 'Mr', 'Mrs', 'Dr', 'Miss', 'Mister']

def get_pdftotext_files(attachment_prefix, data_directory):
    '''
    Return list of pdftotext files to parse
    '''
    files = []
    for root, dirnames, filenames in os.walk(data_directory):
        for filename in filenames:
            if filename.startswith(attachment_prefix) and filename.endswith('.txt'):
                files.append(os.path.join(root, filename))
    return files

def split_pdftotext_file():
    """
    Return 
    """
    pass

def parse_pdftotext_file_chunk():
    """
    Return
    """
    pass

def CMS_2012_0031_79992(parent_comment, files):
    for f in files:
        print f
        h = open(f, 'r')
        c = h.read()
        split = re.split("\f", c)
        primary_key_base = os.path.splitext(os.path.basename(f))[0]
        total_failures = 0
        for j, s in enumerate(split):
            # Convert tuple resulting from sqlite into a list for modification
            new_comment = parent_comment
            r = re.compile(r'^\s*(.*?)\n\s*(.*[A-Z]{2}.*\d{5}-{0,1}\d{0,4}$).*(^[A-Za-z]+ \d{1,2}, \d{4})(.*)', re.MULTILINE|re.DOTALL)
            primary_key = primary_key_base + '-' + str(j)
            # New primary key for documentId
            new_comment['documentId'] = primary_key
            if r.search(s) == None:
                total_failures += 1
                #print s
                #raw_input("Press ENTER to continue...")
                # Consider marking anything with unusually long addresses as failures, then outputting failed IDs to a file for inputting by hand.
                continue
            new_comment['submitterName'] = r.search(s).group(1)
            address = r.search(s).group(2)
            address = re.sub(r'\n', ',', address)
            address = re.sub(r'\s+', ' ', address)
            new_comment['address'] = address
            # Date for collectedDate
            new_comment['receivedDate'] = r.search(s).group(3)
            # Comment text for comment
            new_comment['comment'] = r.search(s).group(4).replace("\n", " ")
            # flag to let us know this comment was parsed from an attachment
            new_comment['parsed'] = 1
            #p = primary_key + ": " + new_comment['submitterName'] + "; " + new_comment['address'] + "; " + new_comment['receivedDate'] + "; " + str(len(new_comment['comment'])) + "; "
            p = "{:<}: {:<25}; {:<60}; {:.30}; {:>15}; {:>5}".format(primary_key, new_comment['submitterName'], new_comment['address'], new_comment['comment'], new_comment['receivedDate'], str(len(new_comment['comment'])))
            print p
        print "Parsed " + str(j) + " comments with " + str(total_failures) + " failures."
        print "Press any key to continue..."
        raw_input()
    return True

def main():
try:
    attachment_prefix = sys.argv[1]
except IndexError:
    print "Usage: " + sys.argv[0] + " <attachment prefix>"
    sys.exit()

conn = get_sqlite_conn(os.path.join(db_dir, db_file))
conn.row_factory = sqlite3.Row
c = conn.cursor()

#attachment_prefix = "test"
attachment_prefix = "CMS-2012-0031-79992"

files = get_pdftotext_files(attachment_prefix, data_dir)

if attachment_prefix == "CMS-2012-0031-79992":
    # exclude files that don't actually use this pattern
    print files
    for i, f in enumerate(files[:]):
        if f.endswith(("CMS-2012-0031-79992_10.txt", "CMS-2012-0031-79992_10b.txt", "CMS-2012-0031-79992_11.txt", "CMS-2012-0031-79992_12.txt", "CMS-2012-0031-79992_13.txt")):
            files.remove(f)
    print files
    parent_comment = get_sql_row_as_dict(c, "documents", "documentId", "CMS-2012-0031-79992")
    CMS_2012_0031_79992(parent_comment, files)
if attachment_prefix == "test":
    parent_comment = get_sql_row_as_dict(c, "documents", "documentId", "CMS-2012-0031-79992")
    CMS_2012_0031_79992(parent_comment, files)
    
conn.close()

if __name__ == "__main__":
    main()


