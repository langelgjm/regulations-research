# Provides functions for getting data from Regulations.gov document and documents APIs

import requests
import time
import os
#import sys
import sqlite3
from retrying import retry
#import code
import smtplib
from email.mime.text import MIMEText
import email.utils
import datetime
from genderize import Genderize
from collections import Counter

###############################################################################
# API documentation here: http://api.data.gov/docs/regulations/
# Import rate limiting information here: http://api.data.gov/docs/rate-limits/
###############################################################################

api_key = 'JlmgAOHcd6FIfLM1Lh9k4AQ691X7oOfx9cyLlpCj'
gmail_username = "gabriel.j.michael@gmail.com"
gmail_app_password = "yykpltkaywbthmsq"

db_file = 'comment_db.sqlite'
log_file = 'make_comment_db.log'
conn = None
ratelimit_threshold = 0

document_url = 'http://api.data.gov:80/regulations/v3/document.json'
documents_url = 'http://api.data.gov:80/regulations/v3/documents.json'
nap = 300
# Tune this parameter to sleep the minimum amount required
catnap = 3.7

def write_log_entry(msg):
    f = open(log_file, 'a')
    s = str(datetime.datetime.now()) + ' ' + msg + '\n'
    f.write(s)
    f.close()
    return None

def setup():
    global conn
    global ratelimit_threshold
    working_directory = "/Users/gjm/Documents/_Works in Progress/Regulations/data/"
    os.chdir(working_directory)
    write_log_entry('Starting up...')
    ratelimit_threshold = set_reg_api_ratelimit()
    conn = sqlite3.connect(db_file)
    return sqlite3.connect(db_file)

def set_reg_api_ratelimit():
    """Get the rate limit limit and set our threshold to 1/10 of that."""
    values = {'api_key':api_key,
              'countsOnly':'1'}
    r = reg_api_request(documents_url, values)
    ratelimit_limit = r.headers['x-ratelimit-limit']
    ratelimit_threshold = int(ratelimit_limit) / 10
    print 'Setting rate limit threshold to ' + str(ratelimit_threshold) + '.'
    return ratelimit_threshold

###############################################################################

def create_document_summary_table(conn):
    """Create a table in which document summaries are stored.
    Several of these columns are not documented in the official API."""
    c = conn.cursor()
    c.execute('''CREATE TABLE document_summaries (agencyAcronym TEXT,
        allowLateComment INTEGER,
        attachmentCount TEXT,
        comment TEXT,
        commentDueDate TEXT,
        commentStartDate TEXT,
        commentText TEXT,
        docketId TEXT,
        docketTitle TEXT,
        docketType TEXT,
        documentId TEXT PRIMARY KEY,
        documentStatus TEXT,
        documentType TEXT,
        numberOfCommentReceived INTEGER,
        openForComment INTEGER,
        organization TEXT,
        postedDate TEXT,
        submitterName TEXT,
        rin TEXT,
        title TEXT)''')

def create_document_table(conn):
    """Create a table in which documents are stored.
    Several of these columns are not documented in the official API."""
    c = conn.cursor()
    # NB the spelling error in numItemsRecieved is intentional
    c.execute('''CREATE TABLE documents (abstract TEXT,
        agencyAcronym TEXT,
        agencyName TEXT,
        allowLateComment INTEGER,
        attachmentCount TEXT,
        cfr TEXT,
        comment TEXT,
        commentCategory TEXT,
        commentDueDate TEXT,
        commentOnDoc TEXT,
        commentStartDate TEXT,
        country TEXT,
        docketId TEXT,
        docketTitle TEXT,
        docketType TEXT,
        documentId TEXT PRIMARY KEY,
        documentStatus TEXT,
        documentSubtype TEXT,
        docSubType TEXT,
        documentType TEXT,
        effectiveDate TEXT,
        federalRegisterNumber TEXT,
        numItemsRecieved TEXT,
        openForComment INTEGER,
        organization TEXT,
        pageCount INTEGER,
        postedDate TEXT,
        receivedDate TEXT,
        rin TEXT,
        startEndPage TEXT,
        status TEXT,
        submitterName TEXT,
        title TEXT,
        trackingNumber TEXT)''')

def create_attachments_table(conn):
    """Create a table in which attachments are stored."""
    c = conn.cursor()
    c.execute('''CREATE TABLE attachments (agencyNotes TEXT, 
        attachmentId INTEGER PRIMARY KEY,
        attachmentOrderNumber INTEGER,
        author TEXT,
        documentId TEXT,
        postingRestriction TEXT, 
        publicationRef TEXT,
        reasonRestricted TEXT,
        title TEXT,  
        FOREIGN KEY (documentId) REFERENCES documents(documentId))''')

def create_fileFormats_table(conn):
    """Create a table in which file formats are stored."""
    c = conn.cursor()
    c.execute('''CREATE TABLE fileFormats (id INTEGER PRIMARY KEY, 
        attachmentId INTEGER,
        url TEXT, 
        FOREIGN KEY (attachmentId) REFERENCES attachments(attachmentId))''')

# Retry our request up to x=5 times, waiting 2^x * 1 minute after each retry
# It would be nice to raise and handle an exception that logged when retries occurred
@retry(stop_max_attempt_number=5, wait_exponential_multiplier=60000)
def reg_api_request( url, values ):
    """Makes a request to the Regulations.gov API, with retries in case of errors"""
    r = requests.get(url, params=values)
    return r

def check_reg_api_ratelimit(r=None, ratelimit_threshold=ratelimit_threshold, nap=nap):
    """Check the Regulations.gov API ratelimit and naps if it is below a certain threshold,
    then calls itself to check again.
    Return True if not below threshold, otherwise recurse."""
    # Normally we pass in a request response, but if not, get one
    if r is None:
        values = {'api_key':api_key,
                  'countsOnly':'1'}
        r = reg_api_request(documents_url, values)
    ratelimit_remaining = r.headers['x-ratelimit-remaining']
    ratelimit_limit = r.headers['x-ratelimit-limit']
    # Be sure to treat this value as a string and convert as necessary!
    print ratelimit_remaining + ' of ' + ratelimit_limit + ' requests remain.'
    if int(ratelimit_remaining) < ratelimit_threshold:
        print 'This is less than the threshold of ' + str(ratelimit_threshold) + '; sleeping for ' + str(nap) + ' seconds...'
        time.sleep(nap)
        values = {'api_key':api_key,
                  'countsOnly':'1'}
        r = reg_api_request(documents_url, values)
        # Recurse with a fresh request
        check_reg_api_ratelimit(r, ratelimit_threshold, nap)
    return True

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
        
def get_reg_api_count(r):
    """Return the number of documents matching a given query."""
    return r.json()['totalNumRecords']

def get_reg_api_docsum_time_est(n, catnap=catnap, rpp=1000):
    """Print and return an estimate of the minimum amount of minutes required to fetch n document summaries.
    Assume 1000 requests per page unless otherwise specified."""
    est = ((n / rpp) * catnap) / 60
    print str(n) + ' documents will be retrieved, requiring at least ' + str(est) + ' minutes.'
    return est

def get_reg_api_doc_time_est(n, catnap=catnap):
    """Print and return an estimate of the minimum amount of minutes required to fetch n documents."""
    est = (n * catnap) / 60
    print str(n) + ' documents will be retrieved, requiring at least ' + str(est) + ' minutes.'
    return est

def reg_api_sleep(catnap=catnap):
    """Sleeps catnap seconds."""
    print 'Sleeping ' + str(catnap) + ' seconds between requests...'
    time.sleep(catnap)
    return True

def parse_reg_api_doc(j):
    """Parses the parsed JSON response from the Regulations.gov API further.
    Returns a parsed response taking care of the dicts within dicts, and eliminating labels."""
    def get_value(k):
        if isinstance(j[k], dict):
            try:
                return j[k]['value']
            except KeyError as e:
                msg = "Error: encountered an unexpected key in the subdictionary " + k + \
                    ". The exception message was: " + str(e) + ". The troublesome parsed JSON response was: " + str(j)
                print msg
                write_log_entry(msg)
                send_error_email(msg)
                # returning None should allow us to proceed normally, just missing whatever data was problematic
                # And that data will be logged, so we don't necessarily lose it.
                return None
        else:
            # If it's not a dictionary, just return it as is
            return j[k]
    def get_title(k):
        return j[k]['title']
    # The only key I've seen that needs non-default handling is this one, but there could be others.
    keys_to_parse = {'commentOnDoc':get_title}
    for k in j:
        try:
            j[k] = keys_to_parse[k](k)
        except KeyError:
            # If the key is not in the list, its for one of two reasons
            # 1. It should'nt be changed or 2. It should be but we don't know the key
            # Get_value(k) will get the value attribute if it is a dictionary
            # But if it isn't it will just return it as is
            j[k] = get_value(k)
    return j

def alter_sqlite_table_add_column(conn, table, col, col_type):
    """Add a column of type to table using connection conn.
    Currently unused. Seems like a bad idea to alter the table in unknown ways."""
    c = conn.cursor()
    c.execute('ALTER TABLE ' + table + ' ADD COLUMN ' + col + ' ' + col_type)
    return None

def insert_reg_api_docs_sqlite(j, conn, table, primary_key, foreign_key=None, foreign_key_value=None):
    """Takes a parsed JSON response from the Regulations.gov API and inserts it into a prepared sqlite database."""
    if isinstance(j, dict):
        # Handle document summary (documents API)
        # For summaries, the JSON response contains a single value which is a list of document summaries.
        # This list contains multiple dictionaries that each represent a single document summary.
        if 'documents' in j.keys():
            l = j['documents']
        # Handle document (document API); technically documentId is optional but in practice can't imagine when it would be lacking
        elif 'documentId' in j.keys():
            # Pass the parsed JSON response elsewhere for further parsing
            # This further parsing discards the labels and gets the values returning a normal dictionary
            j = parse_reg_api_doc(j)
            l = [j]
        else:
            # Would probably be better to raise an exception here
            msg = "Encountered an unexpected parsed JSON response when handling a dictionary."
            print msg
            send_error_email(msg)
            write_log_entry(msg)
            return False
    elif isinstance(j, list):
        if isinstance(j[0], dict):
            # This handles the attachments array in a recursive call
            # One might expect that if there is an attachment, there will be a fileFormat. But
            # this is false - they redact some fileFormats.
            # So instead of testing to see what's inside this list, look at the foreign key, which indicates the parent 
            # in the JSON hierarchy (and thus what the child is).
            if foreign_key == 'documentId':
                l = j
        else:
            # Handles fileFormats array in a recursive call
            # By checking to see if the first element of j is not a dictionary
            # If so, create a list of dictionaries
            l = [dict({'url':i}) for i in j]
    else:
        msg = "Encountered an unexpected parsed JSON response (not a dictionary or list)."
        print msg
        send_error_email(msg)
        return False
    c = conn.cursor()
    try:
        for d in l:
            # Insert key/value pairs one-by-one to avoid having to do custom SQL injection checking on values.
            # Note that this is still vulnerable to injection on keys, which are controlled by Regulation.gov.
            # Insert the primary key to create a record
            if primary_key == 'documentId':
                # cursor.execute expects a tuple, even if it's just of length one
                c.execute('INSERT INTO ' + table + ' (' + primary_key + ') VALUES (?)', (d[primary_key],))
                primary_key_value = d[primary_key]
            else:
                # for records where we don't know the primary key, pass NULL to autoincrement
                c.execute('INSERT INTO ' + table + ' (' + primary_key + ') VALUES (NULL)')
                # Now we have to get the autoincrement primary key back for later use
                # Remember, it returns a tuple but when I use it I just want the value
                (primary_key_value, ) = c.execute('SELECT last_insert_rowid()').fetchone()
                # Since attachments and fileFormats don't come with foreign keys in the response, add one
                d.update({foreign_key:foreign_key_value})
            # Now update the new record with the remaining values
            for k in d:
                # If K is attachments or fileFormats, recursively call this function to put them in the appropriate table
                if k == 'attachments':
                    insert_reg_api_docs_sqlite(d[k], conn, 'attachments', 'attachmentId', 'documentId', d[primary_key])
                elif k == 'fileFormats':
                    insert_reg_api_docs_sqlite(d[k], conn, 'fileFormats', 'id', 'attachmentId', primary_key_value)
                else:
                    try:
                        c.execute('UPDATE ' + table + ' SET ' + k + '=? WHERE ' + primary_key + '=?', (d[k], primary_key_value))
                    except sqlite3.InterfaceError:
                        # For debugging
                        pass
                    except sqlite3.OperationalError as e:
                        # One approach would be to modify the table on the fly, but that seems like a bad idea.
                        # So instead just note the problem and move on.
                        write_log_entry(str(e))
    except UnboundLocalError:
        # For debugging
        pass
    # Save all our hard work
    conn.commit()
    return True

def get_doc_summaries_reg_api( comment_period, document_type, creation_date_range = None, docket_id = None, count_only = 0):
    """Gets document summaries from the Regulations.gov documents API, or a count of documents.
    Doesn't actually return them; instead, calls functions to store them in an sqlite database."""
    values = {'api_key':api_key,
              'countsOnly':'1', # Initially we set this to 1, and later change it to the correct parameter value
              'cp':comment_period,
              'dct':document_type,
              'rpp':1000,
              'po':0}
    # These arguments are sometimes missing, so don't pass them if they're missing
    if creation_date_range is not None:
        values['crd'] = creation_date_range
    if docket_id is not None:
        values['dktid'] = docket_id
    # Make an initial request
    r = reg_api_request(documents_url, values)
    while not check_reg_api_ratelimit(r):
        pass
    # Get the total number of records
    po_total = get_reg_api_count(r)
    # If we want the actual documents, give an estimate of the time required to retrieve them
    if count_only == 0:
        get_reg_api_docsum_time_est(po_total, catnap)
        values['countsOnly'] = str(count_only)
    # Otherwise just return the count of documents
    else:
        print 'There are ' + str(po_total) + ' matching documents.'
        return po_total
    for po in range(0,po_total,1000):
        values['po'] = po
        reg_api_sleep(catnap)
        msg = 'Requesting page ' + str((po / 1000) + 1) + ' of ' + str((po_total / 1000) + 1)  + '...'
        print msg
        write_log_entry(msg)
        try:
            r = reg_api_request(documents_url, values)
        # If it fails, send an e-mail notification and drop to a console rather than exiting
        except:
            msg = 'Re-requesting failed! Stopped at page offset ' + po + " of " + po_total + " documents."
            print msg
            write_log_entry(msg)
            send_error_email(msg)
            return None
        if 'documents' in r.json():
            # Insert the data into the sqlite database
            j = r.json()
            insert_reg_api_docs_sqlite(j, conn, 'document_summaries', 'documentId')
        else:
            msg = "No documents in JSON response! Stopped at page offset " + po + " of " + po_total + " documents."
            print msg
            write_log_entry(msg)
            send_error_email(msg)
            return None
    return j

def get_doc_reg_api(documentId):
    """Gets a single document from the Regulations.gov document API.
    Calls functions to store it in an sqlite database, and returns it."""
    values = {'api_key':api_key,
              'documentId':documentId}
    try:
        r = reg_api_request(document_url, values)
    except:
        msg = 'Re-requesting failed! Failed on document ' + documentId + "."
        print msg
        write_log_entry(msg)
        send_error_email(msg)
        return None
    # While loop not necessary, just makes it clear that we are waiting for this function to be True
    while not check_reg_api_ratelimit(r):
        pass
    # Wait after making this request (and before parsing it)
    reg_api_sleep(catnap)
    try:
        j = r.json()
    except ValueError as e:
        msg = "Couldn't parse JSON! Failed on document " + documentId + '. Error was: ' + str(e)
        print msg
        write_log_entry(msg)
        send_error_email(msg)
        return None
    else:
        insert_reg_api_docs_sqlite(j, conn, 'documents', 'documentId')
    return j

###############################################################################

def main():
    conn = setup()
    #create_document_summary_table(conn)
    #create_document_table(conn)
    #create_attachments_table(conn)
    #create_fileFormats_table(conn)
    c = conn.cursor()
    #get_doc_summaries_reg_api('O', 'PS', None, 'CDC-2014-0012', 0)
    # Test doc with lots of attachments and multiple fileFormats
    #j = get_doc_reg_api('CMS-2012-0031-82979')
#     c.execute('''SELECT documentId FROM document_summaries''')
#     # They arrive as tuples in a list
#     l = c.fetchall()
#     get_reg_api_doc_time_est(len(l), catnap)
#     for (d, ) in l:
#         print d
#         write_log_entry("Getting documentId " + str(d))
#         get_doc_reg_api(d)
#
#     
    c.execute('''SELECT submitterName from documents''')
    l = c.fetchall()
    names = []
    for (n, ) in l:
        fn = n.split()[0]
        # Need to deal with this better! Can't just exclude titles period.
        if fn not in ['Anonymous', 'Dr.', 'Dr']:
            names.append(fn)
    
    def chunks(l, n):
        """ Yield successive n-sized chunks from l."""
        for i in xrange(0, len(l), n):
            yield l[i:i+n]    
    
    # Make a list of lists to pass to genderize
    # The limit is actually higher than 100...
    lists = list(chunks(names, 100))
    genders = []
    for l in lists:
        # Obviously on very large datasets this will have to be improved with retyring, rate limiting, etc.
        genders.extend(Genderize().get([l]))
    #
    cnt = Counter()
    for g in genders:
        if g['gender'] is not None and g['probability'] >= 0.95:
            cnt[g['gender']] +=1
    print cnt
    #    
    conn.close()
    write_log_entry("Shutting down...")
    
if __name__ == "__main__":
    main()