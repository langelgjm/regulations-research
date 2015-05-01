import os
os.chdir("/Users/gjm/Documents/_Works in Progress/Regulations/regulations/")
import configparser
from Common import make_config_dict, get_sqlite_conn
from nameparser import HumanName
import re

excluded_entities = ("Semper Fidelis", "Hobby Lobby", "G.W. Bush", "Obama", "Mikulski", "Sebelius", "Ella", "Jesus", "Jesus Christ", "Christ", "Thomas Jefferson", "Hitler", "Kathleen Sebelius", "Sibelius", "Jeff Fortenberry", "Obama Care", "God", "Hubert Humphrey", "George Washington", "Barack Obama", "James Madison", "Caesar", "Nancy Pelosi", "OBAMA", "Pelosi", "Sebilius", "Biden", "DeWine", "Obamacare", "Joe Biden", "Margaret Sanger", "Roe", "Sebellius", "Wade", "Ackerman", "Mother Teresa", "Abraham Lincoln", "Clinton", "Jefferson", "Jordan Krueger", "Jehovah", "Kathleen Sibelius", "Paul VI", "Sandra Fluke", "Kathleen", "Lincoln", "Martin Luther King", "Stalin", "Madison", "Fortenberry", "Isaiah", "John Paul II", "John Adams", "Richard", "Dolan", "John Quincy Adams", "Mother Theresa", "Timothy Dolan", "Bart Stupak", "Benjamin Franklin", "Ceasar", "Hyde", "Jeremiah", "Patrick Henry", "Sandy Hook", "Sebelious", "Abraham", "Adam", "Griswold", "Mao", "Pope Francis", "Ronald Reagan", "Benedict XVI", "Bill Clinton", "Casey", "Christ Jesus", "Edmund Burke", "Isaac", "Jacob", "Law", "O'Bama", "Oboma", "Pope", "Reg", "Roberts", "AMEN", "Barack Hussein Obama", "Barak Obama", "Ben Carson", "Carson", "Francis", "Kennedy", "Matt Lockshin", "Michael", "Michelle", "Mike DeWine", "Nero", "OBama", "Orwell", "Pease", "Rob Portman", "Alan Sears", "Arnold Joubert", "BERGOGLIO", "Barbara Mikulski", "Fulton Sheen", "Hussein", "John H. Cochrane", "John Paul", "Kathleen Sebelious", "Martin Fox", "Martin Niemoller", "Michael Dorr", "Moses", "President", "Reagan", "Reid", "Richard M. Doerflinger", "Roe V. Wade", "Rosano", "Roy T. Garman", "Soetoro", "U. S.", "-- Thomas Jefferson", "Adolf Hitler", "Alice Paul", "Alinsky", "Augustine", "Cardinal Dolan", "Diocletian", "George III", "Hadassah", "Katherine", "Kathleen Sebellius", "Kathleen Sebilius", "Land of the Free", "Lenin", "Linda", "Martin Luther", "Mrs Sebelius", "Nancy Polosi", "Pope Paul VI", "Pres Obama", "SEBELIUS", "Sibellius", "Thomas Aquinas", "Thomas Paine", "Vivat Jesus", "obama", "--Thomas Jefferson")

def CreateAuthorTable(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS authors (id INTEGER PRIMARY KEY, 
                                                        entityId, gender,
                                                        FOREIGN KEY(entityId) REFERENCES entities(id))''')
    conn.commit()

def ValidateEntity(entity):
    if entity.lower() in (exclude.lower() for exclude in excluded_entities):
        return None
    else:
        return entity

def ValidateFirstName(first_name):
    '''Look at output from HumanName and make sure it doesn't contain 
    anything that would make us think it's not a name.
    '''
    if first_name == '':
        return None
    elif re.search('([0-9]|@)', first_name, re.IGNORECASE) is not None:
        return None
    else:
        # Be sure to return the proper case using title method        
        return first_name.title()

def GetGender(conn, firstname):
    row = conn.execute('''SELECT * FROM genders WHERE name == ? ORDER BY (male + female) DESC''', (firstname, )).fetchone()
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

def InsertGender(conn, documentId, entityId, gender):
    #print("documentId: " + documentId + ", entityId: " + str(entityId) + ", gender: " + gender)    
    conn.execute('''INSERT INTO authors (entityId, gender) VALUES (?, ?)''', (entityId, gender))
    conn.execute('''UPDATE documents SET gender = ? WHERE documentId = ?''', (gender, documentId))
    conn.commit()

def main():   
    config_file = "regulations.conf"
    config = configparser.ConfigParser()
    config.read(config_file)
    myconfig = make_config_dict(config)

    db_file = myconfig['hhs']['db_file']
    conn = get_sqlite_conn(db_file)

    gender_db_file = myconfig['general']['gender_db_file']
    gender_conn = get_sqlite_conn(gender_db_file)
    
    CreateAuthorTable(conn)
    
    rs = conn.execute('''SELECT max(rowid) AS max_rowid, id, documentId, entityString 
                            FROM entities 
                            WHERE entityType == "PERSON" 
                            GROUP BY documentId''').fetchall()
    
    for r in rs:
        entity = ValidateEntity(r["entityString"])
        # Human first name
        if entity:
            first_name = HumanName(entity).first
            if first_name:
                validated_first_name = ValidateFirstName(first_name)
                if validated_first_name:
                    print(validated_first_name)
                    gender = GetGender(gender_conn, validated_first_name)
                    if gender:
                        InsertGender(conn, r["documentId"], r["id"], gender)
        
        # Insert

if __name__ == "__main__":
    main()