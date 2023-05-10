# Author: Saeesh Mangwani
# Date: 16/05/2022

# Description: Deleting and remaking the postgres schema containing pacfish data, to allow for a full database reset.

#%% Loading libraries
import os
from pathlib import Path
os.chdir(Path(__file__).parent.parent.parent)
import psycopg2
from json import load

#%% Resetting the pacfish schema

def reset_pacfish_dbase(creds):

    # Setting the default schema to 'pacfish' unless another was specified in the file
    if 'schema' not in creds.keys():
        creds['schema'] = 'pacfish'

    print('Opening database connection...')
    # Database connection
    conn = psycopg2.connect(
        host = creds['host'],
        database = creds['dbname'],
        user = creds['user'],
        password = creds['password']
    )
    # Creating a cursor
    cursor = conn.cursor()

    print('Resetting schema...')
    # Checking if the pacfish schema exists - creating if not, dropping and remaking if yes
    cursor.execute('DROP TABLE IF EXISTS pacfish.hourly;')
    cursor.execute('DROP TABLE IF EXISTS pacfish.daily;')
    cursor.execute('DROP TABLE IF EXISTS pacfish.hourly_recent;')
    cursor.execute('DROP TABLE IF EXISTS pacfish.station_metadata;')
    cursor.execute('DROP SCHEMA IF EXISTS pacfish;')
    cursor.execute('CREATE SCHEMA pacfish;')
    cursor.execute('GRANT ALL ON SCHEMA pacfish TO postgres, ' + creds['user'] + ';')
    cursor.execute('commit')
    
    print('Closing connection...')
    # Closing and returning
    cursor.close()
    return('Postgres schema reset')
    
#%%
if __name__ == "__main__":
    # Reading credentials from JSON
    creds = load(open('options/credentials.json',))
    reset_pacfish_dbase(creds)