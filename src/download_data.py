"""
Run this script to download the data from Daniel's Dropbox
d.vela1@hotmail.com
"""
import requests
import zipfile
import io
import os
from os import rename
from os import remove
from tqdm import tqdm
import pathlib
import pandas as pd
import dropbox
from dropbox.exceptions import AuthError
def medpix():
    '''
    Download medpix (not working for now, see function definition file comments).

    This function downloads the medpix dataset from Mindkind's google
    drive folder:
    https://drive.google.com/file/d/1-IVgCEGSzPc8NExdWFu8mTTdnhlysU6b/view?usp=sharing
    '''
    # Code below commented because it stopped working after googledrive made
    # changes for now you can download medpix.zip and manually place it in the
    # raw folder before running the unzip function in this method. The file can
    # be found here under the name Production https://drive.google.com/drive/folders/1PyI-R7x9o4pzR-s6pkHb-7XVZWh58s7Z?usp=sharing
# =============================================================================
#     print('downloading medpix')
#     try:
#         gdd.download_file_from_google_drive(file_id='1-IVgCEGSzPc8NExdWFu8mTTdnhlysU6b',
#                                             dest_path=raw_data_path+'medpix.zip',
#                                             unzip=True)
#         print('renaming')
#         rename(raw_data_path+'Production', raw_data_path+'medpix')
#         remove(raw_data_path+'medpix.zip')
#         print('done')
#     except Exception as e:
#         print('Error downloading medpix')
#         print(e)
# =============================================================================
    print('downloading medpix')
    def dropbox_connect():
        """Create a connection to Dropbox."""

        try:
            dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
        except AuthError as e:
            print('Error connecting to Dropbox with access token: ' + str(e))
        return dbx

    def dropbox_download_file(dropbox_file_path, local_file_path):
        """Download a file from Dropbox to the local machine."""

        try:
            dbx = dropbox_connect()
            print('Connection succesfull, starting download...')

            with open(local_file_path, 'wb') as f:
                metadata, result = dbx.files_download(path=dropbox_file_path)
                f.write(result.content)
        except Exception as e:
            print('Error downloading file from Dropbox: try shutting down all kernels and retrying it may be memory error. ')
            
        
    # Download zip from dropbox
    DROPBOX_ACCESS_TOKEN = input("Enter Daniel's dropbox access token:\n")
    dropbox_file_path = '/Global_datasets/medpix.zip'
    zip_path = '../data/raw/medpix.zip'
    dropbox_download_file(dropbox_file_path, zip_path)

    path = '../data/raw/'
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(path)
    remove(zip_path)
    rename(raw_data_path+'Production', raw_data_path+'medpix')
    

def dropbox_connect():
    """Create a connection to Dropbox."""
    DROPBOX_ACCESS_TOKEN = input("Enter Daniel's dropbox access token:\n")
    try:
        dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
    except AuthError as e:
        print('Error connecting to Dropbox with access token: ' + str(e))
    return dbx

def dropbox_download_file(dropbox_file_path, local_file_path):
    """Download a file from Dropbox to the local machine."""

    # try:
    dbx = dropbox_connect()
    print('Connection succesfull, starting download...')

    with open(local_file_path, 'wb') as f:
        metadata, result = dbx.files_download(path=dropbox_file_path)
        f.write(result.content)
    # except Exception as e:
    #     print('Error downloading file from Dropbox: try shutting down all kernels and retrying it may be memory error. ')
        
def download_dataset():
    # Download zip from dropbox
    dropbox_file_path = '/Global_datasets/BreastCancer.zip'
    zip_path = '../data/raw/BreastCancer.zip'
    dropbox_download_file(dropbox_file_path, zip_path)

    path = '../data/raw/BreastCancer'
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(path)
    remove(zip_path)
    # rename(raw_data_path+'Production', raw_data_path+'medpix')

os.makedirs('../data/raw',exist_ok=True)
download_dataset()