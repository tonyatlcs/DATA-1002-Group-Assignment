import json
import zipfile
import os
import gzip
import shutil

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
  
if __name__ == "__main__":
  read = Read()
  read.read_and_save_jsonl_gz_file(
    os.path.join(os.getcwd(), 'src', 'data', 'compressed', 'amazon_data', 'Digital_Music.jsonl.gz'),
    os.path.join(os.getcwd(), 'src', 'data', 'raw_data', 'amazon_data'),
    'digital_music.json'
  )
  read.read_and_save_zip_file(
    os.path.join(os.getcwd(), 'src', 'data', 'compressed', 'movielens_data', 'movie_data_small.zip'), 
    os.path.join(os.getcwd(), 'src', 'data', 'raw_data', 'movielens_data'))
  
  read.read_and_save_jsonl_gz_file(
    os.path.join(os.getcwd(), 'src', 'data', 'compressed', 'amazon_data', 'meta_Digital_Music.jsonl.gz'),
    os.path.join(os.getcwd(), 'src', 'data', 'raw_data', 'amazon_data'),
    'meta_digital_music.json'
  )
