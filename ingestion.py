#!/usr/bin/env python
# coding: utf-8

# In[2]:


###################################################################################################################
# Project: Building a data warehouse with Amazon Redshift
# Name: Isuru Abeysekara
# Date: 5/7/2021
# Program: Provides the infrastructe to extract data from a .csv file from data source and 
# puts it in an S3 bucket.
# Relies on the Source class
###################################################################################################################


# In[2]:


import pyspark, requests, source, boto3, os, source, pandas as pd
from botocore.errorfactory import ClientError
#from source import Source


# In[9]:


class S3Bucket:
    __directory = "./test.csv"
    __bucket = "covid-dat"
    __s3 = boto3.resource('s3')
    __client = boto3.client('s3')
    __key="test.csv"
    
    def __innit__(self):
        pass
    
    def uploadS3(self):
        # downloading data
        source.Source().read()
    
        #Checking if s3 object exists - if it does we "overwrite" the object
    
        try: 
            self.__client.head_object(Bucket=self.__bucket, Key= self.__directory)
        except ClientError:
            # The object does not exist. Proceed to upload the source file as a new bucket
            self.__s3.meta.client.upload_file(self.__directory, self.__bucket, self.__key)
    
        # Object exists. Delete the current object and replace it with the new object 
        self.__client.delete_object(Bucket=self.__bucket, Key=self.__directory)
        self.__s3.meta.client.upload_file(self.__directory, self.__bucket, self.__key)
    
    
    def retrieveS3(self):
        
        # retrieving the response from the s3 bucket via boto3. A status code of 200 is good news.
        response = self.__client.get_object(Bucket=self.__bucket, Key=self.__key)
        
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        
        #error handling done here 
        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            result = pd.read_csv(response.get("Body"))
            return result
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")
            return None
    


# In[ ]:




