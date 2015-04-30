#!/usr/bin/python
# Inserts the Sunlight Foundation comments (JSON) into an SQLite database
import os
import sys
import json
import sqlite3
import smtplib
from email.mime.text import MIMEText
import email.utils
import datetime

gmail_username = "gabriel.j.michael@gmail.com"
gmail_app_password = "yykpltkaywbthmsq"

db_file = 'fcc_comment_db.sqlite'
log_file = 'make_fcc_comment_db.log'
conn = None

def write_log_entry(msg):
    f = open(log_file, 'a')
    s = str(datetime.datetime.now()) + ' ' + msg + '\n'
    f.write(s)
    f.close()
    return None

def setup():
    global conn
    working_directory = "/Users/gjm/Documents/_Works in Progress/Regulations/data/"
    os.chdir(working_directory)
    write_log_entry('Starting up...')
    conn = sqlite3.connect(db_file)
    return sqlite3.connect(db_file)

###############################################################################

def create_fcc_nn_table(conn):
    """Create a table in which FCC initial comments are stored."""
    c = conn.cursor()
    c.execute('''CREATE TABLE fcc_nn (city TEXT,
        zip TEXT,
        proceeding TEXT,
        regFlexAnalysis INTEGER,
        exParte INTEGER,
        preprocessed INTEGER,
        text TEXT,
        pages INTEGER, 
        applicant TEXT,
        disseminated TEXT,
        brief TEXT,
        modified TEXT,
        stateCd TEXT,
        smallBusinessImpact INTEGER,
        fileNumber TEXT,
        dateRcpt TEXT,
        author TEXT, 
        lawfirm TEXT,
        submissionType TEXT,
        id TEXT PRIMARY KEY,
        viewingStatus TEXT,
        email_to TEXT,
        email_subject TEXT,
        reportNumber TEXT,
        daNumber TEXT,
        dateCommentPeriod TEXT,
        dateReplyComment TEXT)''')

def send_email_notification(from_addr, to_addr, subj, msg_text):
    """Send a simple e-mail notification to a single recipient"""
    msg = MIMEText(msg_text)
    msg['To'] = email.utils.formataddr((to_addr, to_addr))
    msg['From'] = email.utils.formataddr(('Python', from_addr))
    msg['Subject'] = subj
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.starttls()
    server.login(gmail_username, gmail_app_password)
    server.sendmail(from_addr, to_addr, msg.as_string())
    server.quit()

def send_error_email(msg = ""):
    """Sends an error notification e-mail"""
    from_addr = 'gabriel.j.michael@gmail.com'
    to_addr = from_addr
    subj = 'An error occurred'
    msg = 'An error occurred. Please log in and check the status of the task. Here is the error message: ' + msg
    # Would be good to handle exceptions here, e.g., in case e-mail is broken
    try:
        send_email_notification(from_addr, to_addr, subj, msg)
    finally:
        pass

def insert_fcc_comment_sqlite(j, conn, table, primary_key, foreign_key=None, foreign_key_value=None):
    """Takes a JSON response loaded from the Sunlight Foundation FCC comments and
    inserts it into a prepared sqlite database.
    """
    if isinstance(j, dict):
        # Insert key/value pairs one-by-one to avoid having to do custom SQL injection checking on values.
        # Insert the primary key to create a record
        # cursor.execute expects a tuple, even if it's just of length one
        c = conn.cursor()
        try:
            c.execute('INSERT INTO ' + table + ' (' + primary_key + ') VALUES (?)', (j[primary_key],))
        except sqlite3.IntegrityError as e:
            msg = "Failed to insert document " + j[primary_key] + "; possibly duplicate primary key? Error message was: " + str(e)
            write_log_entry(msg)
            write_log_entry("ERROR: " + j[primary_key])
            return False
        primary_key_value = j[primary_key]
        # Now update the new record with the remaining values
        for k in j:
            try:
                c.execute('UPDATE ' + table + ' SET ' + k + '=? WHERE ' + primary_key + '=?', (j[k], primary_key_value))
            except sqlite3.InterfaceError as e:
                write_log_entry(str(e))
                return False
            except sqlite3.OperationalError as e:
                write_log_entry(str(e))
                return False
    else:
        msg = "Encountered an unexpected JSON response (not a dictionary)."
        print msg
        send_error_email(msg)
        write_log_entry(msg)
        return False
    c = conn.cursor()
    # Save all our hard work
    # conn.commit() # Actually do this later, every 1000 inserts, to cut down on disk I/O
    return True

def get_sunlight_fcc_comment_files(data_directory):
    """Return a list of JSON files that represent the Sunlight Foundation FCC comment files
    """
    files = []
    for root, dirnames, filenames in os.walk(data_directory):
        for filename in filenames:
            if filename.endswith('.json'):
                files.append(os.path.join(root, filename))
    write_log_entry("Returning list of files...")
    return files
                
###############################################################################

def main():

    conn = setup()
    #create_fcc_nn_table(conn)
    #c = conn.cursor()

    # Provide a full path to the script to indicate what files it should operate on
    data_directory = sys.argv[1]
    #data_directory = "/Users/gjm/Documents/_Works in Progress/Regulations/data/nn_reply_json"
    #data_directory = "/Users/gjm/Documents/_Works in Progress/Net Neutrality/sample"
    print "Getting list of files..."
    files = get_sunlight_fcc_comment_files(data_directory)
    
    def my_gen(files):
        for f in files:
            yield f
        
    print "Inserting files into SQLite database..."
    i = 0 # counter for number of successful inserts
    
    for f in my_gen(files):
        h = open(f, 'r')
        j = h.read()
        #print(json.loads(j))
        h.close()
        r = insert_fcc_comment_sqlite(json.loads(j), conn, "fcc_nn", "id")
        if r == False:
            msg = "Failed to insert " + f
            write_log_entry(msg)
        else:
            i += 1
            if i == 1000:
                conn.commit()
                i = 0
    conn.close()
    write_log_entry("Shutting down...")
    
if __name__ == "__main__":
    main()
