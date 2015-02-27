#!/Users/gjm/anaconda/bin/python
import os
import re
from nameparser import HumanName
from genderize import Genderize, GenderizeException
from retrying import retry
import time
from Common import Logfile, yield_sql_results, update_sql_value, len_GET, get_sqlite_conn, make_config_dict
import ConfigParser

config_file = "regulations.conf"
config = ConfigParser.ConfigParser()
config.read(config_file)
myconfig = make_config_dict(config)

working_directory = myconfig['general']['working_directory']
db_file = myconfig['nn']['nn_db_file']
log_file = myconfig['nn']['nn_genders_log_file']
gender_file = myconfig['nn']['nn_genders_file']
url = myconfig['general']['genderize_url']
conn = None

###############################################################################
# TODO: deduplicate first names before sending to Genderize;
# Then, reassign the genders back to the correct names
# Alternatively, just use the static data 
###############################################################################

def validate_first_name(human_first_name):
    '''Look at output from HumanName and make sure it doesn't contain 
    anything that would make us think it's not a name.
    '''
    if human_first_name == '':
        return None
    elif re.search('([0-9]|@)', human_first_name, re.IGNORECASE) is not None:
        return None
    else:
        return human_first_name

# Retry our request up to x=5 times, waiting 2^x * 1 minute after each retry
@retry(stop_max_attempt_number=5, wait_exponential_multiplier=60000)
def get_genders(l):
    '''
    Use Genderize to get the genders based on a prepared list.
    Return the id, name, and output of Genderize as a tuple
    '''
    l_ids = list(zip(*l)[0])
    l_firstnames = list(zip(*l)[1])
    try:
        l_genders = Genderize().get(l_firstnames)
    except GenderizeException:
        return None
    return zip(l_ids, l_firstnames, l_genders)

def chunk_of(firstnames, limit=8000):
    '''
    Yield lists of first names less than the maximum length that Genderize will accept.
    '''
    if len(firstnames) > 0:
        l = [firstnames.pop()]
        while True:
            params = [('name[]', name) for name in list(zip(*l)[1])]
            if len_GET(url, params) > limit:
                break
            elif len(firstnames) > 0:
                l.append(firstnames.pop())
            else:
                yield l
    else:
        return
    yield l

###############################################################################

def main():

    log = Logfile(os.path.join(working_directory, log_file), disp=True)
    
    conn = get_sqlite_conn(os.path.join(working_directory, db_file))
    c = conn.cursor()
    
    log.write("Executing SELECT query.")
    c.execute('''SELECT id, applicant, author FROM fcc_nn LIMIT 15000''')
    
    log.write("Fetching query results.")
    rs = []
    for result in yield_sql_results(c, fetch_num=10000):
        rs.append(result)
        
    log.write("Extracting first names.")
    firstnames = []
    for r in rs:
        if r[1] is not None:
            name = r[1]
        elif r[2] is not None:
            name = r[2]
        else:
            continue    
        firstnames.append((r[0], validate_first_name(HumanName(name).first.encode('utf-8'))))
    
    # Not actually using the unique list, just making it
    fnames_all = zip(*firstnames)[1]
    fnames_unique = list(set(fnames_all))
    print "There were " + str(len(fnames_all)) + " names, but now there are only " + str(len(fnames_unique)) + " names." 

    log.write("Generating lists to pass to Genderize.")
    
    # Call the generator to create a list of lists to use later
    lists=[]
    try:
        while True:
            lists.append(chunk_of(firstnames).next())
    except StopIteration:
        pass
    
    log.write("Getting genders from Genderize.")
    f = open(gender_file, 'w')
    
    genders = []
    ll = len(lists)
    for i, l in enumerate(lists):
        log.write("On list " + str(i+1) + " of " + str(ll) + "; asking for " + str(len(l)) + " names.")
        g = get_genders(l)
        if g is not None:
            genders.extend(g)
            for h in g:
                if h[2]["gender"] is not None:
                    if h[2]["probability"] >= 0.95:
                        s = h[0] + "," + h[2]["gender"] + "\n"
                        f.write(s)
                        f.flush()
        else:
            log.write("Failed to get genders for the following ids: ")
            for i in list(zip(*l)[0]):
                log.write(i)
        log.write("Sleeping 40 seconds...")
        time.sleep(40) # be nice to the API and don't exceed 2500 requests per day
    
    log.write("Insert genders into SQLite database.")
    i = 0
    for g in genders:
        if g[2]["gender"] is not None:
            if g[2]["probability"] >= 0.95:
                r = update_sql_value(c, "fcc_nn", "id", g[0], "gender", g[2]["gender"], False)
                if r:
                    log.write(str(r))
        i += 1
        if i == 1000:
            i = 0
            conn.commit()     
    
    conn.commit()
    f.close()
    conn.close()
    log.close()
    
if __name__ == "__main__":
    main()
