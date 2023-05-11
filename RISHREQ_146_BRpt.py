#!/usr/bin/env python
# coding: utf-8

# In[60]:


start_date = "1y" #datepicker
focus_date = "0d" #datepicker
country = 'BR' #text
language = 'pt' #text


# In[ ]:


'''
Ticket: RISHREQ-146
Author: Carrie A Minerich
Date: February 16, 2021

brazil country, port lang 
remove new accounts
tech 


 
.*tecnologia|tech|software|ciencia da computacao|programador|analista de suporte|
tecnico de informatica|desenvolvedor|ti|analista de desenvolvimento|sistemas
|analista de bi|analista de inteligência de mercado|analista de sistemas júnior
|analista de sistemas sênior|business intelligence|desenvolvedor front-end|
analista de teste|webmaster|back end developer|desenvolvedor.net|
desenvolvedor back-end|senior back end developer|java|desenvolvedor mobile|
desenvolvedor android|desenvolvedor ios 3|desenvolvedor front-end|desenvolvedor c#|flutter*.

''';


# In[61]:


get_ipython().run_line_magic('reload_ext', 'ishbook')

import iql 
iql.USE_NEW_IQL_CLIENT = True

import pandas as pd 
from datetime import date, timedelta

from contrib import ravenish
from contrib.munge import get_dates
from contrib.fetch.query_tools import credential_collector, database_connector
from contrib.fetch import sanitize_for_query

#to silence ishbook warnings for templateName column , setting on copy. 
import warnings
warnings.filterwarnings("ignore")


# In[62]:


def _validate_user_inputs(user_inputs):
    
    params = {}
    
    temp_focus_date = user_inputs.pop('focus_date')
    params['focus_date'] = get_dates(temp_focus_date)
    
    temp_start_date = user_inputs.pop('start_date')
    params['start_date'] = get_dates(temp_start_date)
    
    input_examples = {
        'country':'Brazil would be BR',
        'language':'Portuguese would be pt'
    }
    
    for key,value in user_inputs.items():
        
        clean_value = str(value).strip()
    
        if not clean_value:
            raise ValueError(f'Invalid parameter: {key}. No {key} found, please enter two character code for intended {key}. e.g. {input_examples[key]}')

        if len(clean_value) != 2 or not clean_value.isalpha():
             raise ValueError(f'Invalid parameter: {key}. {key.title()} must be exactly 2 characters in length and compromised exclusively of letters. e.g. {input_examples[key]}')
            
        params['lower_case_country'] = country.strip().lower()
        params['lower_case_language'] = language.strip().lower()
        params['upper_case_country'] = country.strip().upper()

    
    return params    


# In[ ]:


def _get_db_error_message(error_type, **kwargs):
    
    db_name = kwargs.get('db_name', 'IQL')
    
    persistent_suffix = 'If error is persistent, please reach out to ishbook author.'
    
    error_messages = {
        'db_runtime': f'Oops, we ran into the following unexpected error while trying to query {db_name}:\n{kwargs.get("err",None)}',
        'db_result_type':f'Invalid {db_name} response: excpected results in pandas DataFrame form, instead received {kwargs.get("results_type",None)}. {persistent_suffix}',
        'db_empty_result':f'Invalid {db_name} response: reponse came back empty. Please verify your inputs and try again. {persistent_suffix}',
        'db_unknown':f'Unknown database: {db_name}. Please either pass keyword argument of other_dict_info or update _get_db_conn function to support this database.',
    }
    
    return error_messages[error_type]


# In[ ]:


def _parse_query_results( results, **kwargs ):
    
    if not isinstance(results, pd.core.frame.DataFrame):
        
        error_message = _get_db_error_message('db_result_type', results_type=type(results), **kwargs)
        raise TypeError(error_message)

    if results.empty:
        
        error_message = _get_db_error_message('db_empty_result', **kwargs)
        raise ValueError(error_message)

    return results


# In[ ]:


def _get_iql_results(query, **kwargs):
    
    try:
        results = get_ipython().run_line_magic('iql2', '{query}')
        results = results.drop('count()', axis=1)
        
    except Exception as err:
        
        error_message = _get_db_error_message('db_runtime', err=err)
        raise RuntimeError(error_message)
        
    else:
        return _parse_query_results( results )


# In[ ]:


def _filter_duplicates( df_1, df_2 ):
    return ( df_1[ ~df_1['accountId'].isin( df_2['accountId'] ) ]  )


# In[ ]:


#rename column and change to int32 for memory storage efficency 

def _reformat_results( results ):
  
    results.accountId = results.accountId.astype('int32')
        
    return (results)


# In[ ]:


def get_output( raw_output_df ):

    raw_df = _reformat_results(raw_output_df)    
    
    raw_df['firstName'] = ''
    
    final_df = raw_df.drop_duplicates().dropna().reset_index( drop=True )
    
    #for QA
    #print(final_df.info())
    #print(final_df.head())
    return ravenish.dfToRaven( final_df )


# In[ ]:


#resumedata does not allow to search for broad 'tech' in resume (or inferred fields are unreliable)
#indeedapplyfunnel is challenging to id by such a broad search term (index not really set up that way)

def main(user_inputs):
    
    search_list = 'tecnologia|tech|software|ciencia da computacao|programador|analista de suporte|tecnico de informatica|desenvolvedor|ti|\
            analista de desenvolvimento|sistemas|analista de bi|analista de inteligência de mercado|analista de sistemas júnior|\
            analista de sistemas sênior|business intelligence|desenvolvedor front-end|\
            analista de teste|webmaster|back end developer|desenvolvedor.net|desenvolvedor back-end|senior back end developer|\
            java|desenvolvedor mobile|desenvolvedor android|desenvolvedor ios 3|desenvolvedor front-end|desenvolvedor c#|flutter'
    
    query_parameters = _validate_user_inputs(user_inputs)
    
    # new accounts created last 14 days
    new_accounts_query = f'''FROM passaccount 14d 0d
                            WHERE lang = 'pt_BR'
                            accountid > 0
                            GROUP BY accountid AS accountId'''
    

    #clicked tech jobs
    click_audience_query = f'''FROM clickanalytics {query_parameters["start_date"]} {query_parameters["focus_date"]}
                        WHERE 
                        query =~ '.*{search_list}*.'
                        job_country_code = '{query_parameters["upper_case_country"]}'
                        joblanguage = '{query_parameters["lower_case_language"]}'
                        format != 'unknown'
                        account_id > 0
                        GROUP BY account_id AS accountId'''
    
    # search tech jobs
    search_query = f''' FROM  mobsearch {query_parameters["start_date"]} {query_parameters["focus_date"]},
                                jobsearch {query_parameters["start_date"]} {query_parameters["focus_date"]}
                        WHERE grp != 'privileged'
                        grp != 'spider'
                        page = 1
                        rcv = 'jsv'
                        useragent != 'catchpoint'
                        qnorm =~ '.*{search_list}*.'
                        country = '{query_parameters["lower_case_country"]}'
                        lang = '{query_parameters["lower_case_language"]}'
                        account_id > 0
                        GROUP BY account_id AS accountId'''
    
    # jobalert creation tech
    jobalert_query = f'''FROM jobalertcreation {query_parameters["start_date"]} {query_parameters["focus_date"]}
                        WHERE creationGroups!='privileged'
                        alert_country_code = '{query_parameters["lower_case_country"]}'
                        lang = '{query_parameters["lower_case_language"]}'
                        q =~ '.*{search_list}*.'
                        accountId > 0
                        GROUP BY accountId'''
    
    #view job in tech categories
    view_audience_query = f'''FROM viewjob {query_parameters["start_date"]} {query_parameters["focus_date"]}
                WHERE grp != 'privileged'
                grp != 'spider'
                rcv = "jsv"
                site_country_code = '{query_parameters["lower_case_country"]}'
                lang = '{query_parameters["lower_case_language"]}'
                job_title_category IN ('techsoftware', 'tech', 'techhelp', 'techinfo')
                 account_id > 0
                 GROUP BY account_id AS accountId'''
    
    #jobseeker profile with tech skills 
    profile_audience_query = f'''FROM jobseekerdata {query_parameters["start_date"]} {query_parameters["focus_date"]}
                        WHERE grps!='privileged'
                        country = '{query_parameters["upper_case_country"]}'
                        extractedSkills IN ("4aa53570-9248-46ab-b3b5-f503a756ca8e", "54ffd971-5469-407c-9b2c-689dff41620b", 
                        "5563e45b-1958-48ce-a222-208f86aa25b2", "5695c5e3-fea8-425f-b033-f972a08a3275", 
                        "57888570-9814-4caf-91f4-c84e2c7e3507", "58025ea9-47da-4959-9ba2-90118c84b4a1", 
                        "58957b5b-a0f0-4eaa-ad23-afcc6f19a9bf", "63893905-7171-4a6f-8ecf-df95f7bca284", 
                        "6b7f7d6c-1a66-4a87-b0af-d7967ffca146", "6f8392d4-a4d9-4039-bd0f-fffa67f8a197", 
                        "6eff2485-087f-4b30-8ce0-11e0b859d381", "700bacc6-4436-47cb-bca4-a8a3ba3b3141", 
                        "71aa5bc6-e90e-41b4-bfa9-7ab63e65a5bd", "735f46d7-93f5-41d6-a31b-def8573d3d42", 
                        "769f0837-8e1c-44b6-99be-4a9cdda930f1", "7a73a84b-207c-4109-9c1e-925e331c4afb", 
                        "9772f699-4d4e-4028-b377-31476be1e671", "9c6c3058-92d6-459d-ac7c-34f3de3bc903", 
                        "a091e3cc-93ae-4246-a91c-0520d2c7f4f9", "a5d8de53-bf16-4aff-aef3-503a2b764334", 
                        "b66ed846-98f0-4f75-ade8-ef21f5531648", "d50db9a9-326b-47df-847e-d5fb8b12ea1a", 
                        "d467eb6c-dc7d-41f8-bd02-ca8de2744512", "d57ef302-293f-4405-8fb8-c0ff4e8eb44a", 
                        "d6d98cd3-7087-4148-a4bb-6f6e606fa2c9", "d78872e4-2c6b-457d-af90-7c77309c2de0", 
                        "f0641ea8-e055-48ed-9e56-1bb9e6dd0132", "eecb57f8-39ad-4b5e-9e5f-1c88b88638ef")
                        accountId > 0
                        GROUP BY accountId'''

    #gather click and search audience, merge for first raw results
    click_audience = _get_iql_results(click_audience_query)
    search_audience = _get_iql_results(search_query)
    first_results_df = pd.merge(click_audience, search_audience, how='outer')
    
    
    #gather jobalert audience, merge for second raw results
    jobalert_audience = _get_iql_results(jobalert_query)
    second_results_df = pd.merge(first_results_df, jobalert_audience, how='outer')
    
    
    #gather view tech job audience, merge for third raw results
    view_audience = _get_iql_results(view_audience_query)
    third_results_df = pd.merge(second_results_df, view_audience, how='outer')
    

    #gather jobseeker profile audience, merge for raw results
    profile_audience = _get_iql_results(profile_audience_query) 
    raw_df = pd.merge(third_results_df, profile_audience, how='outer')
    
    
    #new accounts created 14d ago, remove from raw results
    new_accounts_df = _get_iql_results(new_accounts_query)
    final_df = _filter_duplicates(raw_df, new_accounts_df)
    
    #add first name, cannot connect to globaluser so include firstName with null value

    return( get_output( final_df ) )
    


# In[ ]:


user_inputs = {
    'focus_date':focus_date,
    'start_date':start_date,
    'country': country,
    'language': language
}

main(user_inputs)

