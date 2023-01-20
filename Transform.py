import Extract
import pandas as pd 

#Importing the songs_df from the Extract.py
load_df=Extract.return_dataframe()
print(load_df)
# Set of Data Quality Checks Needed to Perform Before Loading
def Data_Quality(load_df):
    #Checking Whether the DataFrame is empty
    if load_df.empty:
        print('No Songs Extracted')
        return False
    
    #Enforcing Primary keys since we don't need duplicates
    if load_df.Series(load_df['played_at']).is_unique:
       pass
    else:
        #The Reason for using exception is to immediately terminate the program and avoid further processing
        raise Exception("Primary Key Exception,Data Might Contain duplicates")
    
    #Checking for Nulls in our data frame 
    if load_df.isnull().values.any():
        raise Exception("Null values found")

Data_Quality(load_df)
    