import numpy as np
import pandas as pd
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas  
from snowflake.connector.pandas_tools import pd_writer



def get_col_types(df: pd.DataFrame, verbose= True) -> str:
    '''
        Helper function to create/modify Snowflake tables; gets the column and dtype pair for each item in the dataframe
        
        args:
            df: dataframe to evaluate
            verbose: choose whether to add prints or logs
    '''    
    # get dtypes and convert to df
    ct = df.dtypes.reset_index().rename(columns={0:'col'})
    ct = ct.apply(lambda x: x.astype(str).str.upper()) # case matching as snowflake needs it in uppers
        
    # only considers objects at this point
    # only considers objects and ints at this point
    ct['col'] = np.where(ct['col']=='OBJECT', 'VARCHAR', ct['col'])
    ct['col'] = np.where(ct['col'].str.contains('DATE'), 'VARCHAR', ct['col'])
    ct['col'] = np.where(ct['col'].str.contains('INT'), 'NUMERIC', ct['col'])
    ct['col'] = np.where(ct['col'].str.contains('FLOAT'), 'FLOAT', ct['col'])
    
    # get the column dtype pair
    l = []
    for index, row in ct.iterrows():
        l.append(row['index'] + ' ' + row['col'])
    string = ', '.join(l) # convert from list to a string object
    string = string.strip()
    
    return string


def create_table(table: str, action: str, col_type: str, df: pd.DataFrame, verbose=True) -> None:
    '''
        Function to create/replace and append to tables in Snowflake
        
        args:
            table: name of the table to create/modify
            action: whether do the initial create/replace or appending; key to control logic
            col_type: string with column name associated dtype, each pair separated by a comma; comes from get_col_types() func
            df: dataframe to load
            verbose: choose whether to add prints or logs
            
        dependencies: function get_col_types(); helper function to get the col and dtypes to create a table
    '''

    # set up connection
    conn = snow.connect(
               account = "DATAROBOT_PARTNER",
               user = "DATAROBOT",
               password = "D@t@robot",
               warehouse = "DEMO_WH",
               database = "SANDBOX",
               schema = "TRAINING",
               role = "PUBLIC")    
    # set up cursor
    if verbose: print('\nSetting up Snowflake connection...')
    cur = conn.cursor()
    if verbose: print('Connection established.')

    if action=='create_replace':
        # set up execute
        if verbose: print('Creating table {} using {} operation...'.format(table, action))
        cur.execute(
            """ CREATE OR REPLACE TABLE 
            """ + table +"""(""" + col_type + """)""") 

        #prep to ensure proper case
        df.columns = [col.upper() for col in df.columns]

        # write df to table
        write_pandas(conn, df, table.upper())
        if verbose: print('Table created.')
        
    elif action=='append':
        # convert to a string list of tuples
        df = str(list(df.itertuples(index=False, name=None)))
        # get rid of the list elements so it is a string tuple list
        df = df.replace('[','').replace(']','')
        if verbose: print('Creating table {} using {} operation...'.format(table, action))
        # set up execute
        cur.execute(
            """ INSERT INTO """ + table + """
                VALUES """ + df + """

            """) 
        if verbose: print('Table created.')
