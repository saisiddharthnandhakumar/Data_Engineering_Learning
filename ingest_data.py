#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


pd.__version__


# In[4]:


pip install sqlalchemy


# In[18]:


pip install psycopg2-binary


# In[3]:


from sqlalchemy import create_engine
from sqlalchemy import text


# In[4]:


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[6]:


connection = engine.connect()


# In[7]:


query = "SELECT 1 as number"
df1 = pd.read_sql(text(query), con = connection)
df1


# In[42]:


#df = pd.read_csv('yellow_tripdata_2021-01.csv')


# In[43]:


#df.head()


# In[49]:


#df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
#df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


# In[52]:


#len(df)


# In[1]:


#print(pd.io.sql.get_schema(df, name = "yellow_tax_data", con = engine))


# In[8]:


df_iter = pd.read_csv('yellow_tripdata_2021-01.csv', iterator = True, chunksize = 100000)


# In[9]:


df = next(df_iter)


# In[10]:


len(df)


# In[11]:


df


# In[12]:


df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


# In[11]:


df.head(0).to_sql(name = "yellow_taxi_data", con = engine, if_exists = "replace")


# In[13]:


get_ipython().run_line_magic('time', 'df.to_sql(name = "yellow_taxi_data", con = engine, if_exists = "append")')


# In[15]:


from time import time
while True:
    t_start = time()
    df = next(df_iter)
    
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    df.to_sql(name = "yellow_taxi_data", con = engine, if_exists = "append")
    
    t_end = time()
    print("Inserted another chunk..., took %.3f seconds" %(t_end - t_start))

