import pandas as pd
import os
from tqdm import tqdm
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

#(5) Save the processed dataset with reports. 
os.makedirs('../data/clean',exist_ok=True)
save_path='../data/clean/BreastCancer.csv'
data[~data['REPORT'].isna()].to_csv(save_path,index=False)
print(f"data prepared succesfully and saved at:  '{save_path}'")