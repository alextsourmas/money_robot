
import datarobot as dr
import pandas as pd
import yaml
import datetime
import requests
from yaml.loader import Loader


def score_buy_and_sell_strategies(all_dataframes_dict: dict):
    '''
    Score via API for just buy and sell strategies
    args: 
        all_dataframes_dict: an output from running the loop prior
    rtypes: 
        buy_response_df: API response df
        sell_response_df: API response df
    dependencies: 
        dependent on the prior functions to get the all_dataframes_dict
        
    '''

    with open("config.yaml", "r") as ymlfile:
        config = yaml.safe_load(ymlfile)

    strategy_list = config['data']['strategy_list']

    #Get configuration settings
    datarobot_endpoint = config['datarobot_settings']['endpoint']
    datarobot_token = config['datarobot_settings']['token'] 
    buy_dataframe_name_str= config['datarobot_api_scoring_settings']['buy_dataframe_name']
    buy_deployment_id_str= config['datarobot_api_scoring_settings']['buy_deployment_id']
    buy_deployment_key_str= config['datarobot_api_scoring_settings']['buy_deployment_key']
    sell_dataframe_name_str= config['datarobot_api_scoring_settings']['sell_dataframe_name']
    sell_deployment_id_str= config['datarobot_api_scoring_settings']['sell_deployment_id']
    sell_deployment_key_str= config['datarobot_api_scoring_settings']['sell_deployment_key']

    #Get dataframes to score
    buy_df = all_dataframes_dict[buy_dataframe_name_str]
    sell_df = all_dataframes_dict[sell_dataframe_name_str]

    buy_df = buy_df.rename(columns={'Stock Splits': 'STOCK_SPLITS'})
    sell_df = sell_df.rename(columns={'Stock Splits': 'STOCK_SPLITS'})
    buy_df.columns = [col.upper() for col in buy_df.columns]
    sell_df.columns = [col.upper() for col in sell_df.columns]


    #Convert data to JSON 
    buy_data = buy_df.to_json(orient='records')

    #API Information
    API_URL = 'https://cfds-ccm-prod.orm.datarobot.com/predApi/v1.0/deployments/{deployment_id}/predictions'    # noqa
    url = API_URL.format(deployment_id=buy_deployment_id_str)
    headers = {
        'Authorization': 'Bearer {}'.format(datarobot_token),
        'Content-Type': "application/json; charset=UTF-8",
        'DataRobot-Key': buy_deployment_key_str,
    }
    params = {}

    #Response Information
    response = requests.post(
            url, data=buy_data, headers=headers,)
    response_object = response.json()
    response_df = pd.json_normalize(response.json()["data"])
    buy_response_df = response_df

    #Print response information 
    print('\nBUY API RESPONSE')
    print('\nResponse Content: ')
    print(response.content)
    print('\nPrediction Values: ')
    print(print(response_df['predictionValues'][0]))

    #Convert data to JSON
    sell_data = sell_df.to_json(orient='records')

    #API Information
    API_URL = 'https://cfds-ccm-prod.orm.datarobot.com/predApi/v1.0/deployments/{deployment_id}/predictions'    # noqa
    url = API_URL.format(deployment_id=sell_deployment_id_str)
    headers = {
        'Authorization': 'Bearer {}'.format(datarobot_token),
        'Content-Type': "application/json; charset=UTF-8",
        'DataRobot-Key': sell_deployment_key_str,
    }
    params = {}

    #Response Information
    response = requests.post(
            url, data=sell_data, headers=headers, params = params
        )
    response_object = response.json()
    response_df = pd.json_normalize(response.json()["data"])
    sell_response_df = response_df

    #Print response information
    print('\nSELL API RESPONSE')
    print('\nResponse Content: ')
    print(response.content)
    print('\nPrediction Values: ')
    print(print(response_df['predictionValues'][0]))

    return buy_response_df, sell_response_df






def run_datarobot_model_factory(dataframe: pd.DataFrame, project_name_prefix: str, verbose=True): 
    '''
    Run the DataRobot model factory within a loop to create multiple projects
    args: 
        dataframe: a dataframe defined by the prior functions
        project_name_prefix: the same as the query string prior - used to create a unique, 
        meaningful name for your project, with a timestamp attached
        verbose: Add useful prints and logs to the code
    rtypes: 
        None
    dependencies: 
        You must feed in a dataset created by another function ready for modeling in DR, 
        and it must adhere to the formats defined in prior functions. Therefore this has 
        to be ran after all the other functions, in that specific order
    '''
    #Define config environment variables 

    #Build the projects
    if verbose: print('Creating project for project {}...'.format(project_name_prefix))
    project_name = f'{project_name_prefix}_{datetime.datetime.now()}'
    if verbose: print(f'Starting project {project_name}')


    #SET FEATURE LIST FOR EVERYTHING EXCEPT YEAR COLUMN DERIVED FROM DATE

    #SET OUT OF TIME VALIDATION, 20 BACKTESTS, 30 DAYS EACH


    temp_project = dr.Project.create(dataframe,\
            project_name = project_name)
    temp_project.set_target(target = 'TARGET', worker_count = -1)

    return None 