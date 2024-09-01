import pandas as pd
import os

class Clean_amazon_data:
  def __init__(self):
    pass

  def aggregate_review_data(self, folder_path: str):
    data_folder_path = folder_path

    datframes = []

    for data_file_name in os.listdir(data_folder_path):
      print(data_file_name)
      if data_file_name.endswith('.json') or data_file_name.endswith('.jsonl'):
        data_file_path = os.path.join(data_folder_path, data_file_name)
        df = pd.read_json(data_file_path, lines=True)

        if len(df) > 5000:
          df = df.sample(n = 5000, random_state = 1)

        datframes.append(df)
    
    big_df = pd.concat(datframes, ignore_index=True)

    print(big_df.head())
    print(big_df.shape)