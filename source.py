#!/usr/bin/env python
# coding: utf-8

# In[1]:


#################################################################################################################
# Project: Building a data warehouse with Amazon Redshift
# Name: Isuru Abeysekara
# Date: 5/7/2021
# Program: Wrapper class for data source
#################################################################################################################


# In[14]:


import requests, os


# In[30]:


class Source:
    __url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
    __response = None
    
    def __innit__(self, readMode):
        pass
        
    def read(self): 
        try:
            __response = requests.get(self.__url)
            with open(os.path.join("/Users/isuruabeysekara/Desktop/Summer/covid_DW", "test.csv"), "wb") as f:
                f.write(__response.content)
        except __response.status_code != 200:
            print("Retrieval error from source")
            
        


# In[ ]:




