import pandas as pd
import pytest
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
load_dotenv()

# Connect to the source and destination databases
source_db = create_engine(os.getenv('SOURCE_DB_CONN'))

destination_db = create_engine(os.getenv('DEST_DB_CONN'))

def client_data_load():
    # read data from source tables
    tablename = 'client'
    client_df = pd.read_sql_table(tablename, source_db)

    # mask PII
    pii_columns = ['email_address', 'name' , 'address', 'phone_number']
    client_df[pii_columns] = client_df[pii_columns].applymap(lambda x: x[0:2] + '*' * (len(x) -3) + x[-1:])

    #add insert timestamp
    client_df['insert_time'] = pd.Timestamp.now()

    # insert into destination database
    client_df.to_sql('client', con=destination_db, if_exists='replace', index=False)

    return client_df
# client_data_load()
print(f"{len(client_data_load())} rows from client table were inserted successfully at {str(pd.Timestamp.now())}")
    
#testing the client_data_load pipeline
def test_client_data_load():
    source_table = 'client'
    destination_table = 'client'
    source_db = create_engine(os.getenv('SOURCE_DB_CONN'))
    destination_db = create_engine(os.getenv('DEST_DB_CONN'))
    
    dest_df= pd.read_sql_table(destination_table,destination_db)
    source_df=pd.read_sql_table(source_table,source_db)
    pii_columns = ['email_address', 'name' , 'address', 'phone_number']

    #check that table destination is not empty
    assert len(dest_df) >1
    # assert len(source_df) == len(dest_df) #check number of records match

    #check if pii columns are masked
    assert dest_df['email_address'].str.contains('\*\*').all() #check whether email is masked
    # assert dest_df['name'].str.contains('\*').all() #check whether name is masked
    assert dest_df['address'].str.contains('\*\*').all() #check whether address is masked
    assert dest_df['phone_number'].str.contains('\*\*').all() #check whether phone_number is masked
        


def enrollee_data_load():
    
    tablename = 'enrollee_profile'
    enrollee_df = pd.read_sql_table(tablename, source_db)

    # mask PII
    pii_columns = ['hmo_id' , 'other_names', 'home_phone_number']
    enrollee_df[pii_columns] = enrollee_df[pii_columns].applymap(lambda x: x[0:2] + '*' * (len(x) -3) + x[-1:])

    #add insert timestamp
    enrollee_df['insert_time'] = pd.Timestamp.now()

    # insert into destination database
    enrollee_df.to_sql('enrollee_profile', con=destination_db, if_exists='replace', index=False)
    
    return enrollee_df
# enrollee_data_load()
print(f"{len(enrollee_data_load())} rows from enrollee_profile table were inserted successfully at {str(pd.Timestamp.now())}")

if __name__ == '__main__':
    pytest.main([__file__])


#testing the enrollee data load pipeline
def test_enrollee_data_load():
    source_table = 'enrollee_profile'
    destination_table = 'enrollee_profile'
    source_db = create_engine(os.getenv('SOURCE_DB_CONN'))
    destination_db = create_engine(os.getenv('DEST_DB_CONN'))
    
    dest_df= pd.read_sql_table(destination_table,destination_db)
    source_df=pd.read_sql_table(source_table,source_db)
    pii_columns = ['hmo_id' , 'other_names', 'home_phone_number']

    #check that table destination is not empty
    assert len(dest_df) >1
    # assert len(source_df) == len(dest_df) #check number of records match

    #check if pii columns are masked
    assert dest_df['hmo_id'].str.contains('\*\*').all() #check whether hmo_id is masked
    assert dest_df['other_names'].str.contains('\*\*').all() #check whether other_names is masked
    assert dest_df['home_phone_number'].str.contains('\*\*').all() #check whether home_phone_number is masked
        

if __name__ == '__main__':
    pytest.main([__file__])

#close database connections
source_db.dispose()
destination_db.dispose()
print('Connection closed')
