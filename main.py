# -*- coding: utf-8 -*-

#Import custom libraries
from money_robot_code import data_engineering
import money_robot_code.database_operations as database_operations


ticker_list = ['SPY'] #Choose a ticker
strategy_list = ['buy'] #Options are "buy", "sell", or "both"
shift_period_list = [3] #How many days are we predicting out? 
move_value_list = [1.5] #What percent do we want the market to move buy to indicate "buy" or "sell"?
table_prefix = 'ALEXT'

pull_data_into_memory = True #Get data for training or testing? Doing things locally? 
save_data_locally = True 
load_data_into_snowflake = True #Load the data from memory into snowflake? 


#Define place to store name of dataframes, and dataframes themselves
all_dataframe_names_list = []
all_dataframes_dict = {}

#Loop through all settings
for ticker in ticker_list: 
    for strategy in strategy_list: 
        for shift_period in shift_period_list: 
            for move_value in move_value_list: 

                #Get strings to name tables, making sure they're in the proper format for Snowflake
                strategy_string = strategy.upper() #convert string to upper to create the table name 
                move_value_string = str(move_value)
                if "." in str(move_value): 
                    move_value_string = move_value_string.replace('.', '_')

                #Create the name of the training table - append to names list
                stock_dataframe_training_table_string = table_prefix+'_'+ticker+'_'+strategy_string+'_'\
                    +'SHIFT'+'_'+str(shift_period)+'_'+'MOVE'+'_'+move_value_string+'_'+'TRAIN'
                all_dataframe_names_list.append(stock_dataframe_training_table_string)

                #Create the name of the testing table - append to names list
                stock_dataframe_testing_table_string = table_prefix+'_'+ticker+'_'+strategy_string+'_'\
                    +'SHIFT'+'_'+str(shift_period)+'_'+'MOVE'+'_'+move_value_string+'_'+'TEST'
                all_dataframe_names_list.append(stock_dataframe_testing_table_string)


                #Get data and engineer features 
                if pull_data_into_memory == True:

                    #Get dataframe, engineer technical indicators, basic reshaping 
                    stock_dataframe = data_engineering.pull_yahoo_data(ticker = ticker, verbose=True)
                    stock_dataframe = data_engineering.create_target_feature(stock_dataframe= stock_dataframe, shift_periods= shift_period,\
                        move_value= move_value, strategy= strategy, verbose=True)
                    stock_dataframe = data_engineering.engineer_technical_indicators(dataframe= stock_dataframe, verbose=True)
                    stock_dataframe = stock_dataframe.rename(columns={'Stock Splits': 'STOCK_SPLITS'})

                    #Create training and testing dataframes
                    stock_dataframe_training = stock_dataframe.iloc[shift_period:]
                    stock_dataframe_training = stock_dataframe_training.reset_index(drop=True)
                    stock_dataframe_testing = stock_dataframe.head(1)

                if load_data_into_snowflake == True: 
                    #Load data into database for TRAINING
                    query_string_train = database_operations.get_col_types(df= stock_dataframe_training)
                    database_operations.create_table(table= stock_dataframe_training_table_string,\
                        action= 'create_replace', col_type = query_string_train, df= stock_dataframe_training)
                    #Load data into database for TESTING 
                    query_string_test = database_operations.get_col_types(df= stock_dataframe_testing)
                    database_operations.create_table(table= stock_dataframe_testing_table_string,\
                        action= 'create_replace', col_type = query_string_test, df= stock_dataframe_testing)

                    all_dataframes_dict[stock_dataframe_training_table_string] = stock_dataframe_training
                    all_dataframes_dict[stock_dataframe_testing_table_string] = stock_dataframe_testing

#Save data locally
if save_data_locally == True: 
    for dataframe_name in all_dataframe_names_list:
        selected_dataframe = all_dataframes_dict[dataframe_name]
        selected_dataframe.to_csv('data/' + dataframe_name + '.csv')


'''
TO DO: 
Immediate Tasks:
    - Create conda requirements file (DONE)
    - Add loops (DONE)
        - Loop through every setting (DONE)
        - if pull_data_into_memory == True (DONE)
            - Save all the related dataframes in a big dictionary, with the dataframes named or labeled for later use (DONE)
        - Optionally save all of those files locally (DONE)
    - Add config files
        - Database
        - DataRobot
        - Model
    - Set up the code to use the config files AND argparse, depending on what you want
    - Save the code in Github
        - Add a readme and instructions to the Github 
    - Build initial deployments end-to-end in DataRobot
        - Create deployments
        - Set up job definitions (run 1 hour before market closes)
        - Create continual retraining
        - Tie in actuals for scoring 
        - Run everything immediately to tesk how it works, check Snowflake

Things to do Later: 
    - Add DataRobot API code, build model factories
        - Find the best setting for each ticker
    - Score via the API as well
    - Build a streamlit application which tracks performance
    -   Deploy it on the cloud
    - Deploy Python code on the cloud, add DevOps pipelines from Github
    
'''