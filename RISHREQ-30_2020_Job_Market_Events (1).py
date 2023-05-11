#!/usr/bin/env python
# coding: utf-8

# In[ ]:


focus_date = "2019-12-28" #datepicker
city = "Washington D.C." #one of: Washington D.C., Phoenix, Sacramento, Seattle, Chicago, Boston, Detroit, Atlanta, Austin, New York City, Sunnyvale, Scottsdale, San Francisco
include_metro_area = "true" #toggle
created_account_X_days_ago = "3" # one of: 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30
created_job_alert_Y_days_ago = "0" #one of: 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30
output_format = "raven_json" #one of: csv_download, raven_json


# In[ ]:


get_ipython().run_line_magic('reload_ext', 'ishbook')

import pandas as pd
import plus
import MySQLdb
import iql 

from contrib import ravenish
from contrib.munge import get_dates
from contrib.fetch import sanitize_for_query 

#HTML for download btn support
from IPython.core.display import HTML


# In[ ]:


'''
    Ticket: RISHREQ-30 Job Market Events 2020
    Author: Carrie Minerich
    Date: December 28, 2019
    
    Audience:
        - Job seekers with accounts created last X days AND/OR created a job alert in the last Y days
        - Want to be able to target cities and larger metro areas. 
        - Located in US, region metro area: Atlanta, D.C., Phoenix, Sacramento, Seattle, Chicago, Boston, Detroit,
           Austin, New York City, Sunnyvale, Scottsdale, San Francisco
           
    Inputs:
        - focus_date: day of email send, defaults to "today"
        - city (with option to include metro area)
        - created_account_X_days_ago: List 0-30 days
        - created_job_alert_Y_days_ago: List 0-30 days
        
    Output formatted for Raven AND CSV:
        -First name
        -Last name
        -Region (or City)
        -State
        -accountId
        -email address for CSV only

    Methodology
    1. Date parameters
    2. Obtain tuple for MSA cities; format state/city upper/lowercase
    3. Find US accounts created X days ago
    4. Find US job alerts created Y days ago 
    5. Join job alert and account creation df for designated city or msa
    6. Pull first, last name and email address based on accountId  
    7. PLUS employer scrub; Format for raven or CSV


''';


# In[ ]:


# date params
focus_dt = get_dates(focus_date).strftime('%Y-%m-%d')

account = int(created_account_X_days_ago)
alert = int(created_job_alert_Y_days_ago)

#for end datetime for IQL query to gather msa string. Max value for end time, X or Y, is 30 days. 
thirty_dt = (pd.to_datetime(focus_date) - pd.Timedelta('30d')).strftime('%Y-%m-%d')

#date for accounts created X days ago 
account_dt = (pd.to_datetime(focus_date) - pd.Timedelta(account, unit='d')).strftime('%Y-%m-%d')
#date for job alerts created Y days ago 
alert_dt = (pd.to_datetime(focus_date) - pd.Timedelta(alert, unit='d')).strftime('%Y-%m-%d')


# In[ ]:


#set variables for queries based on city selected. 
if include_metro_area == "true":

    if city == 'Chicago':
        jlmsa = ''' jlmsa = 'Chicago-Naperville-Elgin, IL-IN-WI' '''  
        state = 'IL'
        ipcity = ''' ipcity IN {msa_tup} '''
        ipRegion = ''' ipRegion IN ('IL', 'IN', 'WI') '''

    elif city == 'Sacramento':
        jlmsa = ''' jlmsa = 'Sacramento--Roseville--Arden-Arcade, CA' '''  
        state = 'CA'
        ipcity = ''' ipcity IN {msa_tup} '''
        ipRegion = ''' ipRegion = 'CA' '''

    elif city == 'Phoenix':
        jlmsa = ''' jlmsa = 'Phoenix-Mesa-Scottsdale, AZ' '''   
        state = 'AZ'
        ipcity = ''' ipcity IN {msa_tup} '''
        ipRegion = ''' ipRegion = 'AZ' '''

    elif city == 'Seattle':
        jlmsa = ''' jlmsa = 'Seattle-Tacoma-Bellevue, WA' ''' 
        state = 'WA'
        ipcity = ''' ipcity IN {msa_tup} '''
        ipRegion = ''' ipRegion = 'WA' '''

    elif city == 'Boston':
        jlmsa = ''' jlmsa = 'Boston-Cambridge-Newton, MA-NH' ''' 
        state = 'MA'
        ipcity = ''' ipcity IN {msa_tup} '''
        ipRegion = ''' ipRegion IN ('MA', 'NH') '''

    elif city == 'Detroit':
        jlmsa = ''' jlmsa = 'Detroit-Warren-Dearborn, MI' '''   
        state = 'MI'
        ipcity = ''' ipcity IN {msa_tup} '''
        ipRegion = ''' ipRegion = 'MI' '''
        
    elif city == 'Washington D.C.':
        jlmsa = ''' jlmsa = 'Washington-Arlington-Alexandria, DC-VA-MD-WV' '''  
        state = 'DC'
        ipcity = ''' ipcity IN {msa_tup} '''
        ipRegion = ''' ipRegion IN ('DC', 'VA', 'MD', 'WV') '''
        
    elif city == 'Atlanta':
        jlmsa = ''' jlmsa = 'Atlanta-Sandy Springs-Roswell, GA' ''' 
        state = 'GA'
        ipcity = ''' ipcity IN {msa_tup} '''
        ipRegion = ''' ipRegion = 'GA' '''

    elif city == 'Austin':
        jlmsa = ''' jlmsa = 'Austin-Round Rock, TX'  '''  
        state = 'TX'
        ipcity = ''' ipcity IN {msa_tup} '''
        ipRegion = ''' ipRegion = 'TX' '''

    elif city == 'New York City':
        jlmsa = ''' jlmsa = 'New York-Newark-Jersey City, NY-NJ-PA'  ''' 
        state = 'NY'
        ipcity = ''' ipcity IN {msa_tup} '''
        ipRegion = ''' ipRegion IN ('NY', 'NJ', 'PA') '''

    elif city == 'Sunnyvale':
        jlmsa = ''' jlmsa = 'San Jose-Sunnyvale-Santa Clara, CA' ''' 
        state = 'CA'
        ipcity = ''' ipcity = 'Sunnyvale' '''  
        ipRegion = ''' ipRegion = 'CA' '''

    elif city == 'Scottsdale':
        jlmsa = ''' jlmsa = 'Phoenix-Mesa-Scottsdale, AZ' '''
        state = 'AZ'
        ipcity = ''' ipcity = 'Phoenix' '''
        ipRegion = ''' ipRegion = 'AZ' ''' 

    elif city == 'San Francisco':
        jlmsa = ''' jlmsa = 'San Francisco-Oakland-Hayward, CA' '''    
        state = 'CA'
        ipcity = ''' ipcity = 'San Francisco' '''
        ipRegion = ''' ipRegion = 'CA' '''
    
else:
    if city == 'Chicago':
        jlmsa = ''' jlmsa = 'Chicago-Naperville-Elgin, IL-IN-WI' ''' 
        state = 'IL'
        ipcity = ''' ipcity = 'Chicago' '''
        ipRegion = ''' ipRegion = 'IL' '''

    elif city == 'Sacramento':
        jlmsa = ''' jlmsa = 'Sacramento--Roseville--Arden-Arcade, CA' '''
        state = 'CA'
        ipcity = ''' ipcity = 'Sacramento' '''
        ipRegion = ''' ipRegion = 'CA' '''

    elif city == 'Phoenix':
        jlmsa = ''' jlmsa = 'Phoenix-Mesa-Scottsdale, AZ' '''   
        state = 'AZ'     
        ipcity = ''' ipcity = 'Phoenix' '''
        ipRegion = ''' ipRegion = 'AZ' '''

    elif city == 'Seattle':
        jlmsa = ''' jlmsa = 'Seattle-Tacoma-Bellevue, WA' ''' 
        state = 'WA'
        ipcity = ''' ipcity = 'Seattle' '''
        ipRegion = ''' ipRegion = 'WA' '''

    elif city == 'Boston':
        jlmsa = ''' jlmsa = 'Boston-Cambridge-Newton, MA-NH' ''' 
        state = 'MA'
        ipcity = ''' ipcity = 'Boston' '''
        ipRegion = ''' ipRegion = 'MA' '''

    elif city == 'Detroit':
        jlmsa = ''' jlmsa = 'Detroit-Warren-Dearborn, MI' '''   
        state = 'MI'
        ipcity = ''' ipcity = 'Detroit' '''
        ipRegion = ''' ipRegion = 'MI' '''

    elif city == 'Washington D.C.':
        jlmsa = ''' jlmsa = 'Washington-Arlington-Alexandria, DC-VA-MD-WV' '''  
        state = 'DC'
        ipcity = ''' ipcity = 'Washington DC' '''
        ipRegion = ''' ipRegion = 'DC' '''

    elif city == 'Atlanta':
        jlmsa = ''' jlmsa = 'Atlanta-Sandy Springs-Roswell, GA' ''' 
        state = 'GA'
        ipcity = ''' ipcity = 'Atlanta' '''
        ipRegion = ''' ipRegion = 'GA' '''

    elif city == 'Austin':
        jlmsa = ''' jlmsa = 'Austin-Round Rock, TX'  '''  
        state = 'TX'
        ipcity = ''' ipcity = 'Austin' '''
        ipRegion = ''' ipRegion = 'TX' '''

    elif city == 'New York City':
        jlmsa = ''' jlmsa = 'New York-Newark-Jersey City, NY-NJ-PA'  ''' 
        state = 'NY'
        ipcity = ''' ipcity = 'New York City' '''
        ipRegion = ''' ipRegion = 'NY' '''

    elif city == 'Sunnyvale':
        jlmsa = ''' jlmsa = 'San Jose-Sunnyvale-Santa Clara, CA' ''' 
        state = 'CA'
        ipcity = ''' ipcity = 'Sunnyvale' '''  
        ipRegion = ''' ipRegion = 'CA' '''

    elif city == 'Scottsdale':
        jlmsa = ''' jlmsa = 'Phoenix-Mesa-Scottsdale, AZ' '''
        state = 'AZ'
        ipcity = ''' ipcity = 'Phoenix' '''
        ipRegion = ''' ipRegion = 'AZ' ''' 

    elif city == 'San Francisco':
        jlmsa = ''' jlmsa = 'San Francisco-Oakland-Hayward, CA' '''    
        state = 'CA'
        ipcity = ''' ipcity = 'San Francisco' '''
        ipRegion = ''' ipRegion = 'CA' '''


# In[ ]:


#Step 2: Obtain a tuple for msa cities  

if include_metro_area == 'true':
    
    msa_q = '''FROM searchablejobs {thirty_dt} {focus_dt}
                         WHERE
                            jobcountry = "US" 
                            {jlmsa} 
                            jlcity != ''
                         GROUP BY jlcity
                         , jladmin1code

                        '''.format(thirty_dt=thirty_dt, focus_dt=focus_dt, jlmsa=jlmsa)

    metro_area_df = get_ipython().run_line_magic('iql2', '{msa_q}')
    metro_area_df.drop('counts', axis=1, inplace=True)
    msa_tup = sanitize_for_query(metro_area_df["jlcity"], dtype = str)

else:
    pass 


# In[ ]:


#Step 3: find US accounts that have created new account X days ago
 
if created_account_X_days_ago == '0':
    print('Reminder: No created account X days ago data is included')
    #return 
        
else:
    new_us_accounts_q = '''
                        FROM passsigninattempt (js_ip_country_code = 'US' {ipCity} accountId != '0') {account_end} {focus_date} as a,
                         passaccount (country = 'US' accountid != '0') as b ALIASING(accountid AS accountId)
                        GROUP BY accountId HAVING b.counts>0 and a.counts>0
                        SELECT DISTINCT(accountId)
                        '''.format(account_end=account_dt, focus_date=focus_dt, ipCity=ipcity)
    
    accounts_df = get_ipython().run_line_magic('iql2', '{new_us_accounts_q}')
    accounts_df.drop('DISTINCT(accountId)', axis=1, inplace=True)
    
    new_us_accounts_df = accounts_df.rename(columns={'accountId HAVING b.counts>0 and a.counts>0':'accountId'})  
    
    #add ipCity and ipRegion to dataframe if created_job_alert_Y_days_ago == '0'
    if created_job_alert_Y_days_ago == '0': 
        new_us_accounts_df['ipCity'] = city
        new_us_accounts_df['ipRegion'] = state
    else:
        pass


# In[ ]:


#Step 4: Find US job alerts created Y days ago.  

if created_job_alert_Y_days_ago == '0':
    print('Reminder: No job alert created Y days ago data is included')
    

else:   
    ja_creation_q = '''
                        FROM jobalertcreation {job_alert_end} {focus_date} 
                         WHERE country = 'us'
                               {ipCity}  
                               {ipRegion} 
                               ipCountry = 'US' 
                               accountId != ''
                         GROUP BY accountId 
                         , ipCity 
                         , ipRegion
                         SELECT DISTINCT(accountId)
                         '''.format(job_alert_end=alert_dt, focus_date=focus_dt,ipRegion=ipRegion, ipCity=ipcity)

    ja_creation_df = get_ipython().run_line_magic('iql2', '{ja_creation_q}')
    ja_creation_df.drop('DISTINCT(accountId)', axis=1, inplace=True)


# In[ ]:


#Step 5: Join job alert and account creation table for designated city or msa. Need to keep all accountids from both dfs. 
if created_job_alert_Y_days_ago == '0':
    alert_account_df = new_us_accounts_df
elif created_account_X_days_ago == '0':
    alert_account_df = ja_creation_df
else:
    alert_account_df = pd.merge(ja_creation_df, new_us_accounts_df, how='outer', on='accountId').drop_duplicates()


# In[ ]:


#Step 6: Connect to database for PII; Find First, Last name and email address 

try: 
    db_username = os.environ['GLOBAL_USERNAME']
    db_password = os.environ['GLOBAL_PASSWORD']

except KeyError:
    db_username = getpass.getpass('LDAP: ')
    db_password = getpass.getpass('Password: ')

globaluserdb_cn = MySQLdb.connect(host= 'dbslave-globaluser.ausoff.indeed.net',
                                 port=3307,
                                 user=db_username,
                                 passwd=db_password)


# In[ ]:


#Retrieve jobseeker email address based on accountId

accids = sanitize_for_query(alert_account_df['accountId'].unique().tolist(), dtype=int)

jobseeker_q = '''
                    SELECT email.emailAddress, email.id AS accountId, name.family_name, name.given_name
                    FROM globaluserdb.tblTosEmail2019b AS email
                    LEFT JOIN globaluserdb.tblaccount_names AS name ON email.id = name.account_id
                    WHERE email.id IN {accids}
                    
                    '''.format(accids=accids)

jobseeker_df = pd.read_sql_query(jobseeker_q, globaluserdb_cn, params={'accids':accids}, coerce_float = False)


# In[ ]:


#merge DFs , remove accountids not in alert_account_df
final_df = pd.merge(alert_account_df, jobseeker_df, how='outer', on=['accountId'])

#confirm no duplicate accountIds. 
#final_df.drop_duplicates(subset='accountId', keep=False, inplace=True)


# In[ ]:


#Formatting for dataframe 
final_df.rename(columns = {'ipCity':'City'}, inplace = True)
final_df.rename(columns = {'ipRegion':'State'}, inplace = True)
final_df.rename(columns = {'given_name':'firstName'}, inplace = True)
final_df.rename(columns = {'family_name':'lastName'}, inplace = True)


# In[ ]:


#fill in known missing city and state values. 
values = {'City': city, 'State': state}
final_df = final_df.fillna(values)


# In[ ]:


#PLUS to scrub advertiser/employer accountIds
scrub = plus.get_advertiser_info(final_df['accountId'],intype='acctid')
scrub = scrub[~scrub['advertiser_id'].isnull()].reset_index()

final_df = final_df[~final_df['accountId'].isin(scrub['accountId'])]


# In[ ]:


## Function to create a HTML button to download dataframe as CSV.

def dl_button(df, fname, btn_text='Download data'):
    download_url = df.to_s3(filename = fname)
    link_string = '''<a href="{0}" download="output" style="text-decoration:none; color:white">{1}</a>'''.format(download_url, btn_text)
    button_obj = '''<button type="button" class="btn btn-primary" style="margin:10px 0px 10px 0px">{0}</button>'''.format(link_string)
    return display(HTML('''
      <div class="container">
      <div class="row">
      <div class="col-md-12">
        {button}
      </div>
      </div>
      </div>
      '''.format(button=button_obj)))


# In[ ]:


# format for either csv_data or Raven output and generate:

if output_format is "csv_download":
    #output csv download
    dl_button(final_df, "2020JobMarketEvents.csv")
else:
    final_df.drop('emailAddress', axis=1, inplace=True)
    ravenish.dfToRaven(final_df)

