import zipfile
import os
import gzip
import pandas as pd

class Read:
  def __init__(self):
    pass

  # Read .jsonl.gz file
  def read_and_save_jsonl_gz_file(self, file_path: str, destination_decompressed_file_path: str, destination_file_name: str):
    print(f'Beginning to read {file_path}...')
    if not os.path.exists(destination_decompressed_file_path):
      os.makedirs(destination_decompressed_file_path)
    
    if os.path.exists(f"{destination_decompressed_file_path}/{destination_file_name}"):
      print('File already exists')
      return 'File already exists'
      
    with gzip.open(file_path, 'rt', encoding='utf-8') as f_in:
      with open(f"{destination_decompressed_file_path}/{destination_file_name}", 'w', encoding='utf-8') as f_out:
        print('writing line...')
        for line in f_in:
            f_out.write(line)
    
    return 'File successfully decompressed'

  
  def read_and_save_zip_file(self, file_path: str, destination_decompressed_file_path: str):
    if not os.path.exists(destination_decompressed_file_path):
      os.makedirs(destination_decompressed_file_path)
    else:
      print('folder already exists')
      return 'folder already exists'

    with zipfile.ZipFile(file_path, 'r') as zip_ref:
      zip_ref.extractall(destination_decompressed_file_path)
    return True
  
  def read_parquet_file(self, file_path: str):
    df = pd.read_parquet(file_path)
    print(df)
    return df

