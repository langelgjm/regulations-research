# Inserts the Sunlight Foundation comments (JSON) into an SQLite database
import os

# fix later
working_directory = "/Users/gjm/Documents/_Works in Progress/Regulations/bin/"
os.chdir(working_directory)

from make_comment_db import write_log_entry, send_email_notification, send_error_email, reg_api_sleep, set_reg_api_ratelimit, reg_api_request, check_reg_api_ratelimit, get_reg_api_doc_time_est
from make_comment_db import gmail_username, gmail_app_password, document_url, catnap, nap
import sqlite3
import requests
import urlparse
import mimetypes
import smtplib
from email.mime.text import MIMEText
import email.utils
import datetime

api_key = "0tcKkW2IEWIDfTQa9hCr39OrEj1zgUKkKccnk8dE"
db_file = 'hhs_comment_db.sqlite'
log_file = 'hhs_comment_db.log'
download_url = "https://api.data.gov/regulations/v3/download"
download_dir = "/Users/gjm/Documents/_Works in Progress/Regulations/data/hhs/attachments/"
conn = None

def setup():
    global conn
    working_directory = "/Users/gjm/Documents/_Works in Progress/Regulations/data/hhs/"
    os.chdir(working_directory)
    write_log_entry('Starting up...')
    mimetypes.init()
    conn = sqlite3.connect(db_file)
    return sqlite3.connect(db_file)

def get_reg_api_url_filename(url):
    url_parsed = urlparse.urlparse(url)
    url_parsed_queries = urlparse.parse_qs(url_parsed.query)
    documentId = url_parsed_queries['documentId'][0]
    attachmentNumber = url_parsed_queries['attachmentNumber'][0]
    contentType = url_parsed_queries['contentType'][0]
    filename = '{}_{}.{}'.format(documentId, attachmentNumber, contentType)
    return filename

def get_attachments(conn, documentId, download_dir):
    """
    Download all the attachments from documentId, placing them into download_dir. Return number of files downloaded.
    """
    c = conn.cursor()
    c.execute('''SELECT url FROM fileFormats WHERE attachmentId IN (SELECT attachmentId FROM attachments WHERE documentId = ?)''', (documentId,))
    urls = c.fetchall()
    downloaded = 0
    for (url,) in urls:
        url_parsed = urlparse.urlparse(url)
        url_parsed_queries = urlparse.parse_qs(url_parsed.query)
        documentId = url_parsed_queries['documentId'][0]
        attachmentNumber = url_parsed_queries['attachmentNumber'][0]
        contentType = url_parsed_queries['contentType'][0]
        # Download the file using an API request
        write_log_entry('Downloading file ' + url)
        r = get_attachment_reg_api(documentId, attachmentNumber, contentType)
        # Examine headers to get filename and mimetype
        write_log_entry(documentId + ': ' + r.headers["content-disposition"])
        extension = mimetypes.guess_extension(r.headers["content-type"])
        if extension is None:
            extension = "." + contentType
        filename = '{}_{}{}'.format(documentId, attachmentNumber, extension)
        f = open(os.path.join(download_dir, filename), 'wb')
        f.write(r.content)
        f.close()
        downloaded += 1
    #
    if downloaded == len(urls):
        return downloaded
    else:
        write_log_entry('Failed to fetch ' + str(len(urls) - downloaded) + ' out of ' + str(len(urls))  + 'attachments for documentId ' + documentId)
        return -1
 
def get_attachment_reg_api(documentId, attachmentNumber, contentType):
    """
    Get a single attachment from the Regulations.gov document API.
    """
    values = {'api_key':api_key,
              'documentId':documentId,
              'attachmentNumber':attachmentNumber,
              'contentType':contentType}
    try:
        r = reg_api_request(download_url, values)
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
    return r

###############################################################################

def main():

    conn = setup()
    c = conn.cursor()
    c.execute('''SELECT documentId FROM documents WHERE attachmentCount > 1''')
    
    def sqlite_fetch_iter(cursor, arraysize=1000):
        while True:
            rs = cursor.fetchmany(arraysize)
            if not rs:
                break
            for r in rs:
                yield r
    
    for r in sqlite_fetch_iter(c):
        (documentId, ) = r
        get_attachments(conn, documentId, download_dir)
    
    conn.close()
    write_log_entry("Shutting down...")
    
if __name__ == "__main__":
    main()
