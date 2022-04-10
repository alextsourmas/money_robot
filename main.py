# -*- coding: utf-8 -*-

#Import libraries
import yaml
from yaml.loader import Loader

#Import custom libraries
from money_robot_code import data_engineering
import money_robot_code.database_operations as database_operations

#Load the config file with settings
with open("config.yaml", "r") as ymlfile:
    config = yaml.safe_load(ymlfile)

#Load data settings from config file
ticker_list = config['data']['ticker_list']
shift_period_list = config['data']['shift_period_list']
move_value_list = config['data']['move_value_list']
strategy_list = config['data']['strategy_list']
table_prefix = config['data']['table_prefix']

#Load app settings from config file
save_data_locally = config['app']['save_data_locally']
load_data_into_snowflake = config['app']['load_data_into_snowflake']
model_factory = config['app']['model_factory']
api_scoring = config['app']['api_scoring']


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

                #Load training and testing dataframes into the dictionary, with names
                all_dataframes_dict[stock_dataframe_training_table_string] = stock_dataframe_training
                all_dataframes_dict[stock_dataframe_testing_table_string] = stock_dataframe_testing

                #Load data into snowflake
                if load_data_into_snowflake == True: 
                    #Load data into database for TRAINING
                    query_string_train = database_operations.get_col_types(df= stock_dataframe_training)
                    database_operations.create_table(table= stock_dataframe_training_table_string,\
                        action= 'create_replace', col_type = query_string_train, df= stock_dataframe_training)
                    #Load data into database for TESTING 
                    query_string_test = database_operations.get_col_types(df= stock_dataframe_testing)
                    database_operations.create_table(table= stock_dataframe_testing_table_string,\
                        action= 'create_replace', col_type = query_string_test, df= stock_dataframe_testing)

                #IF model factory is turned on, create projects for each dataset in DataRobot
                if model_factory: 
                    None

#If API scoring is turned on, score one of the testing files on a selected deployment 
if api_scoring: 
    None


#Save data locally
if save_data_locally == True: 
    for dataframe_name in all_dataframe_names_list:
        selected_dataframe = all_dataframes_dict[dataframe_name]
        selected_dataframe.to_csv('data/' + dataframe_name + '.csv')





'''
TO DO: 
Immediate Tasks:
    - (DONE)Create conda requirements file 
    - (DONE) Add loops 
        - (DONE) Loop through every setting 
        - (DONE) if pull_data_into_memory == True 
            - (DONE) Save all the related dataframes in a big dictionary, with the dataframes named or labeled for later use (DONE)
        - (DONE) Optionally save all of those files locally 
    - Add config files (DONE)
        - Database (DONE)
        - DataRobot
        - Appp (DONE)
    - (NOT NECESSARY) Set up the code to use the config files AND argparse, depending on what you want
    - (DONE) Save the code in Github 
        - Add a readme and instructions to the Github (USE PIP FOR INSTALL, NOT CONDA)
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
    - Dockerize so that there isn't problems with the requirements file 
    - Deploy Python code on the cloud, add DevOps pipelines from Github
    
'''