import os
from .src import db
import json
import requests
import pandas as pd
# from dotenv import load_dotenv

# load_dotenv()

AUTH_KEY = os.getenv('AUTH_KEY')
MAIN_FOLDER = os.getenv('MAIN_PATH')

# ENDPOINTS URLS

    # BULK ENDPOINT NEEDS COORDINATES !!!!
    # BULK_URL = f'https://api.optimoroute.com/v1/create_or_update_orders?key={AUTH_KEY}'
CREATE_ORDER_URL = f'https://api.optimoroute.com/v1/create_order?key={AUTH_KEY}'
START_PLANNING_URL = f'https://api.optimoroute.com/v1/start_planning?key={AUTH_KEY}'


# QUERY LOADS

query_path = os.path.join(MAIN_FOLDER, 'queries')

query_jobs_path = os.path.join(query_path, 'tomorrow_jobs.sql')
query_services_path = os.path.join(query_path, 'supplied_services.sql')

query_jobs = open(query_jobs_path, 'r').read()
query_services = open(query_services_path, 'r').read()

services = db.sql_to_df(
    query_services, use_live=True
    ).set_index('ID'
    ).to_dict()['Service']


# JOBS DURATIONS TABLE

times_path = os.path.join(query_path, 'jobs_time.csv')
times = pd.read_csv(times_path)


# FUNCTIONS

def get_services(row:str)->str:
    """Functions to get the services names from the services codes

    Args:
        row (str): List of the services codes to be 'translate'

    Returns:
        str: String coma separated of all the different services that are going to be done
    """    
    return ','.join([services.get(int(item),'NA') for item in row])

def clean_row(row:dict, bulk_format:bool=False, test:bool=False)->dict:
    """_summary_

    Args:
        row (dict): Dictionary with all the information of a row from the query (tomorrow's jobs)
        bulk_format (bool, optional): Boolean feel to change the format of the final dictionary. Bulk endpoint need a diferent data structure. Defaults to False.

    Returns:
        dict: Final dictionary with the rigth structure for sending jobs to optimo
    """    
    order_dict = row.to_dict()
    order_dict['notes'] = f"""{row['job_type']} - {row['price']} - {row['services']} -"""
    del order_dict['price'], order_dict['services'], order_dict['job_type']
    order_dict['location'] = {
        'notes': "TEST0",
        'address': f"{order_dict['location_postcode_address']}", # {order_dict['location_address']} 
        }
    del order_dict['location_postcode_address'], order_dict['location_address']
    if not bulk_format:
        order_dict['location']['acceptPartialMatch'] = True
        # BULK ENDPOINT needs to include coordinates info
        # order_dict['location']['latitude'] = ???
        # order_dict['location']['location'] = ???
    if order_dict['locksmith_email'] != 'wgtklogistics@soterps.com':
        order_dict['assignedTo'] = {
            'externalId': order_dict['locksmith_email'],
        }
    else:
        order_dict['assignedTo'] = {
            'externalId': 'datateambot@soterps.com',
        }
    del order_dict['locksmith_email']
    order_dict['duration'] = get_job_duration(order_dict['SpareKey'], order_dict['LocksmithSuppliedServicesIds'])
    del order_dict['SpareKey'], order_dict["LocksmithSuppliedServicesIds"]
    if test:
        # FOR TESTING
        order_dict['orderNo'] = 'TESTING_' + order_dict['orderNo']
    else:
        # REAL
        order_dict['orderNo'] = order_dict['orderNo']
    return order_dict

def get_job_duration(SpareKey:bool, LocksmithSuppliedServicesIds:list)->int:
    """Function to determine the standar time for a job.
    This function uses a csv with all the times for the different locksmith services.

    Args:
        SpareKey (bool): The client have or not a spare key
        LocksmithSuppliedServicesIds (list): List of all the services that are going to be performed

    Returns:
        int: It returns an integer between 20 and 180 according to the job services.
    """    
    tmp_time = times[times['ID'].astype(str).isin(LocksmithSuppliedServicesIds)].copy()
    if SpareKey:
        tmp_time['time'] = tmp_time['SPARE KEY']
    else:
        tmp_time['time'] = tmp_time['AKL']
    tmp_time = tmp_time.groupby(['group'], as_index=False).agg({'time': 'max'})
    if 8 in tmp_time['group'] and tmp_time.shape[0] > 1:
        tmp_time = tmp_time[tmp_time['group']!=8]
    duration = int(tmp_time['time'].sum())
    if duration < 20:
        duration = 20
    elif duration > 180:
        duration = 180
    
    return duration

def clean_df_tomorrow_jobs(df:pd.DataFrame)->tuple[str, pd.DataFrame]:
    """First cleaning aproach to the raw query of jobs. It set the defaul values for the fields needed by the endpoint.
    It also change the format of some fields.

    Args:
        df (pd.DataFrame): Table with the raw information from SQL

    Returns:
        tuple:
            [
            str: String with the date of the jobs to schedule
            pd.DataFrame: final table after the cleaning process
            ] 
    """    
    df['LocksmithSuppliedServicesIds'] = df['LocksmithSuppliedServicesIds'].apply(lambda x: x.split(','))
    df['services'] = df['LocksmithSuppliedServicesIds'].apply(get_services)
    df['customField3'] = df['customField3'].apply(lambda x: x.split(',')[1])
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    df['operation'] = 'CREATE' # CREATE, UPDATE, SYNC, MERGE
    df['job_type'] = ['SK'if row else 'AKL' for row in df['SpareKey']]
    df['type'] = 'T'
    df['location_address'] = df['location_address'].fillna('')
    return (list(df['date'])[0] , df)

def post_job_2_optimo(order:dict)->dict:
    """Function for posting one job on optimo

    Args:
        order (dict): Dictionary with all the information needed to post a job in optimo
            {
                "orderNo": "str", Care ref plus date
                "date": "YYYY-MM-DD",
                "email": "name@email.com", Email to the area
                "phone": "07700000000", Customer phone
                "customField1": "AA00 AAA", Vehicle reg
                "customField2": "Make", Car maker
                "customField3": " Model", Car model
                "customField4": "VIN", Car VIN
                "operation": "CREATE", Operation type
                "type": "T", Type task 
                "notes": "AKL - 0.00 - Warranty -", Custom field depends on needs
                "location": {
                    "address": "address", Job address and post code
                    "acceptPartialMatch": true Boolean
                },
                "assignedTo": {
                    "externalId": "locksmith@soterps.com" Locksmith email
                },
                "duration": 00 Duration in minutes for the task
            }
    Returns:
        dict: Dictionary with the result after posting the jobs, it can be success or failure
    """
    result = {}
    result['orderNo'] = order['orderNo']
    result['optimoId'], result['success'] = None, False
    data = json.dumps(order)
    re = requests.post(CREATE_ORDER_URL, data=data)
    if re.status_code == 200:
            re = re.json()
            if re['success']:
                result['optimoId'] = re['id']
                result['success'] = True
            else:
                result.update(re)
                result["address"] = order["location"]["address"]
                # result.update(order)
    else:
        result.update(re)
        result["address"] = order["location"]["address"]
        # result.update(order)
    return result

def main(test:bool=False)->dict:
    """_summary_

    Args:
        test (bool): _description_. Defaults to True.

    Returns:
        dict: Dictionary with the result after the execution.
        {
            'message': message with details of execution
            'plannig': bool with the result of the planning
            'success': Dataframe with all the jobs that are in the system
            'fail': Dataframe with the jobs that could not be posted on optimo
        }
    """      
    fail_df = None
    success_df = None
    planning_status = None
    message = None
    df = db.sql_to_df(query_jobs, use_live=True)
    if not df.empty:
        df.fillna('', inplace=True)
        date, df = clean_df_tomorrow_jobs(df)
        print(date)
        results = []
        for order in [clean_row(row, test=test) for _, row in df.iterrows()][:]:
            results.append(post_job_2_optimo(order))
        results = pd.DataFrame(results).sort_values('success', ascending=True)
        fail_df = results[~results['success']].drop(columns=['optimoId'])
        already_done = fail_df[fail_df['code'] == 'ERR_ORD_EXISTS']
        if not already_done.empty:
            already_done = already_done[['orderNo', 'success']]
        else:
            already_done = pd.DataFrame(columns=['orderNo', 'success'])
        already_done['optimoId'] = 'already_system'
        fail_df = fail_df[fail_df['code'] != 'ERR_ORD_EXISTS'][['orderNo', 'success', 'code', 'message']]
        success_df = results[results['success']][['orderNo', 'success', 'optimoId']]
        success_df = pd.concat([success_df, already_done])

        # PLANNING // ASSINGNED TO DRIVER 
        re = requests.post(START_PLANNING_URL, data=json.dumps({'date': date}))
        if re.status_code == 200:
                re = re.json()
                planning_status =re['success']
                if not re['success']:
                    message = f"Error while planning: {re['code']}"
                else:
                    message = 'Planning done'
        else:
            # print('1',re.reason)
            # print('2',re.json())
            pass   
    else:
        message = "There were no orders to be posted on Optimo"
    return {
        'message': message,
        'plannig': planning_status,
        'success': success_df,
        'fail': fail_df
        }

if __name__ == '__main__':
    result = main(test=True)

    if not result['success'].empty:
        success_df_str = result['success'].to_string(index=False)
    else:
        success_df_str = ''

    if not result['fail'].empty:
        fail_df_str = result['fail'].to_string(index=False)
    else:
        fail_df_str = ''

    print(f"""RESULT: {result['message']}
Planning success: {result['plannig']}
\nFail Jobs: {result['fail'].shape[0]}
{fail_df_str}
\nSuccess Jobs: {result['success'].shape[0]}
{success_df_str}""")

    # ############################# BULK ENDPOINT NEEDS COORDINATES !!!! #################################
    # # data = {
    # #     'orders' : [clean_row(row, bulk_format=True) for _, row in df.iterrows()][:2]
    # #     }
    # # re = requests.post(BULK_URL, data=json.dumps(data))
    # # if re.status_code == 200:
    # #     print(json.dumps(re.json(), indent=4))
    # # else:
    # #     print(re.reason)
    # #     print(re.json())