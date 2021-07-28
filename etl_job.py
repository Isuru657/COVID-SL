#!/usr/bin/env python
# coding: utf-8

# In[1]:


###################################################################################################################
# Project: Building a data warehouse with Amazon Redshift
# Name: Isuru Abeysekara
# Date: 5/7/2021
# Program: Performs data cleaning on the data to be warehoused. 
# Relies on the ingestion class
##################################################################################################################


# In[1]:


from ingestion import S3Bucket


# In[2]:


def pullData():
    resp = list()
    S3Bucket().uploadS3()
    res = S3Bucket().retrieveS3()
    if res is None:
        resp.append(-1)
        resp.append(None)
        print(f"Data retrieval unsuccessful")
    else:
        resp.append(0)
        resp.append(res)
    return resp


# In[3]:


def getFinal(raw):
    resp = list()
    if raw is None:
        resp.append(-1)
        resp.append(None)
        
    else:
        south_asia = ["India", "Pakistan", "Bangladesh", "Sri Lanka", "Nepal", "Bhutan", "Maldives"]
        south_asia_df = raw[raw['location'].isin(south_asia)]
        south_asia_df['total_deaths_perc'] = south_asia_df['total_deaths']/south_asia_df['total_cases']
        south_asia_df['new_deaths_perc'] = south_asia_df['new_deaths']/south_asia_df['new_cases']
        final_df = south_asia_df[['iso_code', 'continent', 'location', 'date', 'total_cases', 'new_cases', 'extreme_poverty', 'handwashing_facilities',
        'hospital_beds_per_thousand', 'life_expectancy', 'human_development_index', 'excess_mortality', 'total_deaths_perc', 'new_deaths_perc']]
        final_df.fillna(0)
        resp.append(0)
        resp.append(final_df)
    
    return resp


# In[4]:


import pandas_redshift as pr

def redshiftUpload(final_df):
    if final_df is None:
        return -1
        
    pr.connect_to_redshift(dbname = "dev",
                        host = "covid-cluster-1.cww2lgerboph.us-east-2.redshift.amazonaws.com",
                        port = "5439",
                        user = "isuru123",
                        password = "Hellohello123")

    pr.connect_to_s3(aws_access_key_id = "AKIAQBRRGFDJOI24UAO6",
                aws_secret_access_key = "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",
                bucket = "covid-dat")
    pr.pandas_to_redshift(data_frame = final_df,
                        redshift_table_name = 'staging')
    
    pr.close_up_shop()
    return 0


# In[5]:


def errorHandler():
    _, _, tb = sys.exc_info()
    traceback.print_tb(tb) 
    tb_info = traceback.extract_tb(tb)
    filename, line, func, text = tb_info[-1]

    print('An error occurred on line {} in statement {}'.format(line, text))
    sys.exit(1)
    


# In[6]:


import sys
import traceback

if __name__ == "__main__":
    
    raw = pullData()
    try:
        assert raw[0] == 0
    except AssertionError:
        errorHandler()
    
    final = getFinal(raw[1]) 
    try:
        assert final[0] == 0
    except AssertionError:
        errorHandler()
        
    resp = redshiftUpload(final[1])
    try:
        assert resp == 0
    except AssertionError:
        errorHandler()
    
    exit(0)
    


# In[ ]:




