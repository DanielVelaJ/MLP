"""
This script takes the dataset, matches the ids to the reports and creates a split using a seed. 

"""
import pandas as pd
import os
from tqdm import tqdm
seed=4 #Seed for choosing elements at random 

# Prepare progress indication widget
print('preparing data...')
tqdm.pandas()

# Define some pandas apply functions
def search_report(ID,reports):
    """
    To be applied as a pandas map on a series. It searches the ID in the 
    reports and returns the corresponding report. 
    
    Args: 
    ID(str): The string usually in IDYEAR column from dataset. 
    rerpots(str): A dataframe of all the reports, must contain an IDYEAR column. 
    Returns:
    report(str): The report string corresponding to the given ID. If the
        report is not found it returns None. 
    """
    if (reports.IDYEAR==ID).any():
        return reports['Report'].loc[reports.IDYEAR==ID].to_string(index=False)
    else:
        return None
    

path_2014='../data/raw/BreastCancer/2014.xlsx'
path_2015='../data/raw/BreastCancer/2015.xlsx'
path_data='../data/raw/BreastCancer/data.xlsx'
# Load data
df_2014=pd.read_excel(path_2014)
df_2015=pd.read_excel(path_2015)
data=pd.read_excel(path_data)

#(1) Avoid repeated IDs
df_2014=df_2014.loc[~df_2014.duplicated('ID')]
df_2015=df_2015.loc[~df_2015.duplicated('NID')]

#(2) Form a unique id+year identifier 'IDYEAR' for each report
df_2014['IDYEAR']=df_2014.ID.apply(lambda x: str(x)+'_2014')
df_2015['IDYEAR']=df_2015.NID.apply(lambda x: str(x)+'_2015')

#(3) Merge all reports
reports=pd.concat([df_2014,df_2015],axis=0)[['IDYEAR','Report']]

#(4) Add reports to the dataset
data['REPORT']=data.IDYEAR.progress_apply(lambda x:search_report(x,reports))

#(5) Avoid empty reports
data=data[~data['REPORT'].isna()] 

#(6) Shuffle data. 
data=data.sample(frac=1,random_state=seed)

#(7) Select only relevant columns. 
cols=['REPORT','BRCAEvent','Year','AGE','DaysToEvent']
data=data[cols]

#() Save the processed dataset with reports. 
os.makedirs('../data/clean',exist_ok=True)
save_path='../data/clean/BreastCancer.csv'
data.to_csv(save_path,index=False)

# (7) Peform split.
train_fraction=0.7
validation_fraction=0.3
test_fraction=0.0

n=len(data) # total number of samples
train_n=int(n*train_fraction)
validation_n=int(n*validation_fraction)
test_n=int(n*test_fraction)
train_data=data[0:train_n]
validation_data=data[train_n:train_n+validation_n]
test_data=data[train_n+validation_n:
                        train_n+validation_n+test_n]

# (8) Save splits but only take reports and labels
train_save_path='../data/clean/BreastCancer_train.csv'
validation_save_path='../data/clean/BreastCancer_validation.csv'
test_save_path='../data/clean/BreastCancer_test.csv'

train_data.to_csv(train_save_path,index=False)
validation_data.to_csv(validation_save_path,index=False)
test_data.to_csv(test_save_path,index=False)

print(f"data prepared succesfully and saved at:  '../data/clean/'")