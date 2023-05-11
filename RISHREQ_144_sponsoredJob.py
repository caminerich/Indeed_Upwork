#!/usr/bin/env python
# coding: utf-8

# In[18]:


start_date = "20d" #datepicker
focus_date = "12d" #datepicker
raven_focus_date = '1d' #datepicker 
raven_start_date = '2d' #datepicker


# In[3]:


'''
Ticket: RISHREQ-144
Author: Carrie A Minerich
Date: February 4, 2021

only returning advertiserId and company name. 


Two ishbooks:
1st: US_en_SMBX_MM_SponsorJobs. NOT IN spent but IN optimized. 
2nd: NOT in spent NOT IN optimized: 


''';


# In[4]:


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


# In[5]:


def _validate_user_inputs(user_inputs):
    
    params = {}
    
    temp_raven_focus_date = user_inputs.pop('raven_focus_date')
    params['raven_focus_date'] = get_dates(temp_raven_focus_date)
    
    temp_raven_start_date = user_inputs.pop('raven_start_date')
    params['raven_start_date'] = get_dates(temp_raven_start_date)
    
    temp_focus_date = user_inputs.pop('focus_date')
    params['focus_date'] = get_dates(temp_focus_date)
    
    temp_start_date = user_inputs.pop('start_date')
    params['start_date'] = get_dates(temp_start_date)
    
    return params    


# In[6]:


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


# In[7]:


def _parse_query_results( results, **kwargs ):
    
    if not isinstance(results, pd.core.frame.DataFrame):
        
        error_message = _get_db_error_message('db_result_type', results_type=type(results), **kwargs)
        raise TypeError(error_message)

    if results.empty:
        
        error_message = _get_db_error_message('db_empty_result', **kwargs)
        raise ValueError(error_message)

    return results


# In[8]:


def _get_iql_results(query, **kwargs):
    
    try:
        results = get_ipython().run_line_magic('iql2', '{query}')
        results = results.drop('count()', axis=1)
        
    except Exception as err:
        
        error_message = _get_db_error_message('db_runtime', err=err)
        raise RuntimeError(error_message)
        
    else:
        return _parse_query_results( results )


# In[9]:


def _filter_duplicates( df_1, df_2 ):
    return ( df_1[ ~df_1['advertiser_id'].isin( df_2['advertiser_id'] ) ]  )


# In[10]:


#rename column and change to int32 for memory storage efficency 

def _reformat_results( results ):
  
    results.advertiser_id = results.advertiser_id.astype('int32')
    
    results = results.rename(columns={'advertiser_id':'advertiserId'})

    return (results)


# In[16]:


def get_output( raw_output_df ):

    final_df = _reformat_results(raw_output_df)
    
    #audience 1: DID NOT spent and DID optimize. Receive US_en_SMBX_MM_SponsorJobs template
    
    #audience 2: DID NOT spend and DID NOT optimize: Start drip with first of 3 day. 
    #second ishbook and set it up 3x with each different template
    
    final_df = final_df.drop_duplicates().dropna().reset_index( drop=True )
    
    #for QA
    #print(final_df.info())
    #print(final_df.head())
    return ravenish.dfToRaven( final_df )


# In[12]:


#audience 1: DID NOT spent and DID optimize. Receive US_en_SMBX_MM_SponsorJobs template

def main(user_inputs):
    
    query_parameters = _validate_user_inputs(user_inputs)

    #1: base audience
    base_audience_query = f'''
    FROM ravenemail {query_parameters["raven_start_date"]} {query_parameters["raven_focus_date"]}
    WHERE clientName="Dradis" 
    campaign="smbx_mm_job_live"
    GROUP BY advertiser_id    '''
    
    audience = _get_iql_results(base_audience_query)
    
    #2 advertisers who spent
    advertiser_spent_query = f'''
    FROM searchablejobs {query_parameters["start_date"]} {query_parameters["focus_date"]}
    WHERE dupeStatus!=1 
        job_visibility_level in ('organic','jobalert') or 
        (job_visibility_level='sponsored' and sponVisibility='spon_active')
        agg_job_id in 
            (FROM dradis_job2 {query_parameters["start_date"]} {query_parameters["focus_date"]} 
            WHERE is_sponsor_all =0 daily_total_revenue_cents >0
            job_hash in 
                    (from dradisJobCreateOrUpdate {query_parameters["start_date"]} {query_parameters["focus_date"]}
                    WHERE action = 'UPDATE' advertiser_id!=39741782 job_hash in 
                            (from dradiswizardtiming {query_parameters["start_date"]} {query_parameters["focus_date"]} WHERE edit =1  tosheet =~ "sponsor.*"
                            jobhash in (
                                    FROM litedradisjob {query_parameters["start_date"]} {query_parameters["focus_date"]}
                                    WHERE advertiser_id!=39741782 jobagedays=0 AND 
                                    job_hash in (FROM midmarketAutoclaimNewJob {query_parameters["start_date"]} {query_parameters["focus_date"]}
                                                WHERE success = 1 AND advertiserId!=39741782 AND
                                                advertiserId in 
                                                    (FROM midmarketAdvertiserEligibility {query_parameters["start_date"]} {query_parameters["focus_date"]}
                                                    WHERE tokenizedGroups in ("midmarket_autoclaim_applyurl1") event="NEW" 
                                                    group by advertiserId)
                                                claimedAggJobId != ''
                                                GROUP BY dradisJobHash)
                                    GROUP BY job_hash)
                            group by jobhash) 
                  group by job_hash) 
          group by agg_job_id_tokenized) 
    GROUP BY sourceid___claimedadvertiserid as advertiser_id '''
    
    spent_audience = _get_iql_results(advertiser_spent_query)
    
    #3 advertisers who optimized
    advertiser_optimized_job_query = f'''                 
    FROM dradisJobCreateOrUpdate {query_parameters["start_date"]} {query_parameters["focus_date"]}
    WHERE action = 'UPDATE'
    advertiser_id!=39741782 
    job_hash in 
        (from dradiswizardtiming {query_parameters["start_date"]} {query_parameters["focus_date"]} where edit =1  
        jobhash in (
            FROM litedradisjob {query_parameters["start_date"]} {query_parameters["focus_date"]}
            WHERE advertiser_id!=39741782 jobagedays=0 AND 
            job_hash in 
                (FROM midmarketAutoclaimNewJob {query_parameters["start_date"]} {query_parameters["focus_date"]}
                WHERE success = 1 AND advertiserId!=39741782 AND
                   advertiserId in 
                        (FROM midmarketAdvertiserEligibility {query_parameters["start_date"]} {query_parameters["focus_date"]}
                        WHERE tokenizedGroups in ("midmarket_autoclaim_applyurl1") event="NEW" 
                        group by advertiserId)
                   claimedAggJobId != ''
                   GROUP BY dradisJobHash)
            GROUP BY job_hash) 
        group by job_hash)
    group by advertiser_id  '''
    
    optimized_audience = _get_iql_results(advertiser_optimized_job_query)
    
    
    #audience 1: DID NOT spent and DID optimize. Receive US_en_SMBX_MM_SponsorJobs template
    
    #4 advertisers IN base audience AND IN optimized audience. 
    base_optimized_audience_df = pd.merge(audience, optimized_audience, how='inner', on='advertiser_id')
    
    #remove advertisers that spent 
    raw_df = _filter_duplicates(base_optimized_audience_df, spent_audience)
    
    #company name 
    #final_df = _get_company_name(raw_df)
    
    if raw_df.empty:
        advertiser_id_list = [-1] 
    else: 
        advertiser_id_list = sanitize_for_query(raw_df.advertiser_id.unique().tolist(), dtype=int)
        
    company_name_query = f'''FROM dradis_job2 15d 0d 
    WHERE advertiser_id IN {advertiser_id_list}
    GROUP BY company, advertiser_id '''
    
    company_name_df = _get_iql_results(company_name_query)
    
    final_df = pd.merge(raw_df, company_name_df, how='outer', on='advertiser_id')

    #raw_df.info()
    return( get_output( final_df ) )
    


# In[19]:


user_inputs = {
    'focus_date':focus_date,
    'start_date':start_date,
    'raven_focus_date':raven_focus_date,
    'raven_start_date':raven_start_date
}

main(user_inputs)


# In[ ]:




