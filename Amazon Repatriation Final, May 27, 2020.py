#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Amazon Repatriation Automation
## Jamie needs to concatenate sales data received from markets into one CSV file
### Notes for Jamie: Run 'UPC excel formulas' for costs and sales UPCs
#### Concatenate ICLA and Classical Sheet into one CSV, with 'UPC excel formula' ran
##### Jamie needs to send over Cost Feed from Amazon, Concatenated Sales Feed from markets, ICLA exceptions / Classical Sheet and International Catalogue
###### create dashboard from the data


# In[2]:


#importing libraries

import pandas as pd
import numpy as np
from pandas import Series,DataFrame
from google.cloud import bigquery


# In[3]:


#importing sales data

sales = pd.read_csv('/Users/steinerickson/Desktop/UMG/Amazon Repatriation/Test Data May 2020/Amazon Repat - Amazon Data.csv', dtype={'ean': str})


# In[4]:


#importing cost data

costs = pd.read_csv('/Users/steinerickson/Desktop/UMG/Amazon Repatriation/Test Data May 2020/Amazon Cost Feed Combined.csv', dtype={'UPC': str})


# In[5]:


#show imported costs data

costs.head()


# In[6]:


#show imported costs data

sales.head()


# In[7]:


#concatenating inventrory owner groups, vendor groups for streamlined data

sales.loc[sales['inventory_owner_group'] == 'UK', 'inventory_owner_group_combined'] = 'UK'
sales.loc[sales['inventory_owner_group'] == 'www.amazon.co.uk', 'inventory_owner_group_combined'] = 'UK'
sales.loc[sales['inventory_owner_group'] == 'DE', 'inventory_owner_group_combined'] = 'DE'
sales.loc[sales['inventory_owner_group'] == 'www.amazon.de', 'inventory_owner_group_combined'] = 'DE'
sales.loc[sales['inventory_owner_group'] == 'IT', 'inventory_owner_group_combined'] = 'IT'
sales.loc[sales['inventory_owner_group'] == 'www.amazon.it', 'inventory_owner_group_combined'] = 'IT'
sales.loc[sales['inventory_owner_group'] == 'FR', 'inventory_owner_group_combined'] = 'FR'
sales.loc[sales['inventory_owner_group'] == 'www.amazon.fr', 'inventory_owner_group_combined'] = 'FR'
sales.loc[sales['inventory_owner_group'] == 'ES', 'inventory_owner_group_combined'] = 'ES'
sales.loc[sales['inventory_owner_group'] == 'www.amazon.es', 'inventory_owner_group_combined'] = 'ES'
sales.loc[sales['inventory_owner_group'] == 'Pan-EU SCO', 'inventory_owner_group_combined'] = 'Pan-EU SCO'
sales.loc[sales['vendor_group'] == 'UMG UK', 'vendor_group2'] = 'UK'
sales.loc[sales['vendor_group'] == 'UMG DE', 'vendor_group2'] = 'DE'
sales.loc[sales['vendor_group'] == 'UMG FR', 'vendor_group2'] = 'FR'
sales.loc[sales['vendor_group'] == 'UMG IT', 'vendor_group2']  ='IT'
sales.loc[sales['vendor_group'] == 'UMG ES', 'vendor_group2'] ='ES'
sales.loc[sales['vendor_group'] == 'UMG AT', 'vendor_group2'] ='AT'
sales.loc[sales['vendor_group'] == '3rd Party', 'vendor_group2'] = '3rd Party'


# In[8]:


#creating movement key

sales['movement'] = sales['inventory_owner_group_combined'] + sales['vendor_group']


# In[9]:


#show new table

sales.head()


# In[10]:


#splitting up strings of UPC Amazon delivers to us

new = sales['ean'].str.split("/", n =-1, expand = True) 
sales["ean1"]= new[0] 
sales["ean2"]= new[1] 
sales["ean3"]= new[2] 
sales["ean4"]= new[3] 
sales["ean5"]= new[4] 
sales.drop(columns =["ean"], inplace = True) 
sales


# In[11]:


#left merging sales and costs data removing any N/A values 

combo = sales.merge(costs, how='inner', left_on=["ean1"or"ean2"or"ean3"or"ean4"or"ean5","vendor_group2"], right_on=["UPC","Region"])


# In[12]:


#showing new data

combo


# In[13]:


#removing any rows with Transfer Price below .30 euros

combo.drop(combo.index[combo['Transfer Price'] < .30], inplace = True)


# In[14]:


#showing data

combo


# In[15]:


#importing international catalogue data

international_catalogue = pd.read_csv('/Users/steinerickson/Desktop/UMG/Amazon Repatriation/Test Data May 2020/physical_catalogue.csv', dtype={'UPC': str})


# In[16]:


#showing international catalogue data

international_catalogue.head()


# In[17]:


#merging our dataset and international catalogue to return Rep Owner for each UPC

combo_cat = pd.merge(combo,international_catalogue[['UPC','Rep Owner']],left_on="ean1"or"ean2"or"ean3"or"ean4"or"ean5", right_on=['UPC'], how='left')


# In[18]:


#showing new data

combo_cat.head()


# In[19]:


#dropping all rows with Rep Owner of Concord

combo_cat.drop(combo_cat.index[combo_cat['Rep Owner'] =='%Concord%'], inplace = True)


# In[20]:


#dropping all rows with Rep Owner of Caroline

combo_cat.drop(combo_cat.index[combo_cat['Rep Owner'] =='%Caroline%'], inplace = True)


# In[21]:


#show new data

combo_cat.head()


# In[22]:


#importing exception list

exception_list = pd.read_csv('/Users/steinerickson/Desktop/UMG/Amazon Repatriation/Test Data May 2020/exception_list.csv', dtype={'UPC': str})


# In[23]:


#show exception list

exception_list.head()


# In[24]:


##delete duplicates from exception list

exception_list.drop_duplicates(subset ="UPC", 
                     keep = False, inplace = True) 


# In[25]:


##show exception list
exception_list


# In[26]:


combo_cat


# In[27]:


#merging data with exception list to find artist exceptions

data_with_exception = pd.merge(combo_cat,exception_list[['UPC','Exception']],left_on="ean1"or"ean2"or"ean3"or"ean4"or"ean5", right_on=['UPC'], how='left')


# In[28]:


##show data with exception

data_with_exception


# In[29]:


##assigning repat percentage, changing Nan to other 50%
data_with_exception['Exception'] = data_with_exception['Exception'].fillna(.50)


# In[30]:


##assigning repat percentage, classical 60%, artist exception 40%, other 50%

data_with_exception.loc[data_with_exception['Exception'] == .5, 'repat_%'] = '.5'
data_with_exception.loc[data_with_exception['Exception'] == 'Artist ICLA', 'repat_%'] = '.4'
data_with_exception.loc[data_with_exception['Exception'] == 'Classical', 'repat_%'] = '.6'


# In[31]:


##show data

data_with_exception


# In[32]:


##change repat % column to float for equations
data_with_exception['repat_%'] = data_with_exception['repat_%'].astype(float)


# In[33]:


##Repatriation equation bracket net unit price times repat% price brackets minus transfer price

data_with_exception['Repatriation'] = (data_with_exception['Net Unit Price'] * data_with_exception['repat_%']) - data_with_exception['Transfer Price'] 


# In[34]:


##show data
data_with_exception.head()


# In[35]:


##Repatriation percentage equation  bracket repatiation divded by dealer price bracket times one hundred

data_with_exception['Repatriation_Percentage_%'] = (data_with_exception['Repatriation'] / data_with_exception['Dealer Price']) *100 


# In[36]:


##show data

data_with_exception.head()


# In[37]:


##total repatriation percentage equation stock movements times repatriation 

data_with_exception['Total_Repatriation'] = data_with_exception['unpacked_units'] * data_with_exception['Repatriation'] 


# In[38]:


##show data

data_with_exception.head()


# In[39]:


#removing any rows with negative Total_Repatriation total 

data_with_exception.drop(data_with_exception.index[data_with_exception['Total_Repatriation'] < 0], inplace = True)


# In[40]:


##show data

data_with_exception.head()


# In[41]:


##remove rows with any NA for Total_Repatriation

data_with_exception = data_with_exception[data_with_exception['Total_Repatriation'].notna()]


# In[42]:


##show final data

data_with_exception


# In[62]:


## rename columns for google big query upload 
data_with_exception = data_with_exception.rename(columns={'Report Type': 'Report_Type', 'Rep Owner': 'Rep_Owner', 'Transfer Price':'Transfer_Price','Dealer Price':'Dealer_Price','Net Unit Price':'Net_Unit_Price','Currency ':'Currency','repat_%':'repat_percentage','Repatriation_Percentage_%':'Repatriation_Percentage_Final'})


# In[63]:


data_with_exception.head()


# In[65]:


##remove rows with any NA for inventory owner group

data_with_exception = data_with_exception[data_with_exception['inventory_owner_group'].notna()]


# In[66]:


###upload data to BigQuery

data_with_exception.to_gbq('commercial_affairs_analytics.amazon_repatriation_may27_2020', 'umg-uk')


# In[ ]:




