import requests
from io import BytesIO
import zipfile
import pathlib

def fetch_table(table_id, table_language='en'):
    """
    Fetches the table from the Stats Can server
    
    Parameters
    ----------
    table_id : string
      ID for the table from the Stats Can catalog
    
    table_language: string, optional
      language version to get
    
    Returns
    -------
    Zip file
    
    """
    
    table_url=f"https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/{table_id}/{table_language}"
    table_info = requests.get(table_url)    
    result = table_info.json()
    try:
        if result['status'] == 'SUCCESS':
            zip_file=requests.get(result['object'], stream=True)
    except:
        if 'message' in result:
            raise Exception(result['message'])
        raise Exception('Error fetching info')
    z = zipfile.ZipFile(BytesIO(zip_file.content))  
    return z

def extract_files(zipfile, path='dataset_download/'):
    """
    Extracts the files from the zipfile to the local disk
    
    Parameters
    ----------
    zipfile : zipfile.Zipfile
    
    path : string, optional
      the place to store the files on the disk
      
    """

    zipfile.extractall(path)    

def download_table(table_id, table_language='en', path='dataset_download/'):
    """
    Downloads the files and extract to disk.
    
    Parameters
    ----------
    table_id : string
      ID for the table from the Stats Can catalog
    
    table_language: string, optional
      language version to get
    
    path : string, optional
      the place to store the files on the disk
   
    """

    zipfile = fetch_table(table_id, table_language)
    extract_files(zipfile, path)

def wds_fetch_table(table_id, table_language='en', path='dataset_download/'):
    """
    Check if the table is present on disk, otherwise downloads it.
    
    Parameters
    ----------
    table_id : string
      ID for the table from the Stats Can catalog
    
    table_language: string, optional
      language version to get

    path : string, optional
      the place to store the files on the disk
    
    """
    meta_file = pathlib.Path(f'{path}{table_id}_metadata.csv')
    file = pathlib.Path(f'{path}{table_id}.csv')
    if (file.exists ()) and (meta_file.exists()):
        print("File found")
    else:
        print("File not found, downloading...")
        download_table(table_id, table_language=table_language, path=path)
        print("Download finished")