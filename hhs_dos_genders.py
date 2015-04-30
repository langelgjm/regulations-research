import os
os.chdir("/Users/gjm/Documents/_Works in Progress/Regulations/regulations")
import sqlite3
from Common import get_sqlite_conn, make_config_dict
import configparser
import re
from nameparser import HumanName

config_file = "regulations.conf"
config = configparser.ConfigParser()
config.read(config_file)
myconfig = make_config_dict(config)

db_file = myconfig['dos']['db_file']
gender_db_file = myconfig['general']['gender_db_file']
conn = None

###############################################################################

def first_name(full_name):
    first_name = HumanName(full_name).first    
    if first_name == '':
        return None
    elif re.search('([0-9]|@)', first_name, re.IGNORECASE) is not None:
        return None
    else:
        # Be sure to return the proper case using title method
        return first_name.title()

def get_gender(cur, firstname):
    row = cur.execute('''SELECT * FROM genders WHERE name == ? ORDER BY (male + female) DESC''', (firstname, )).fetchone()
    if row:
        if row['male'] > row['female']:
            gender = 'm'
        elif row['male'] < row['female']:
            gender = 'f'
        else:
            gender = None
        
        try:
            prop = row['male'] / (row['male'] + row['female'])
            if 0.05 < prop < 0.95:
                gender = None
        except ZeroDivisionError:
            gender = None
    else:
        gender = None
    return gender
    
###############################################################################

def main():
    conn = get_sqlite_conn(db_file)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    gender_conn = get_sqlite_conn(gender_db_file)
    gender_conn.row_factory = sqlite3.Row
    gender_c = gender_conn.cursor()
    
    # Get ids and names
    rs = c.execute('''SELECT documentId, submitterName FROM documents''')
    
    genders = {}
    while rs:
        row = rs.fetchone()
        if not row is None:
            firstname = first_name(row['submitterName'])
            if not firstname is None:
                gender = get_gender(gender_c, firstname)
                if not gender is None:
                    genders[row['documentId']] = gender
                    #c.execute('''UPDATE documents SET gender = ? WHERE documentId = ?''', (gender, row['documentId']))
                    #conn.commit()
        else:
            break
    
    def gender_generator():
        for k in genders.keys():
            yield (genders[k], k)
    
    c.executemany('''UPDATE documents SET gender = ? WHERE documentId = ?''', gender_generator())    
        
    conn.commit()
    conn.close()
    gender_conn.close()
    
if __name__ == "__main__":
    main()
