import pandas as pd
import os

class Clean_amazon_data:
  def __init__(self):
    pass

  def aggregate_json_data(self, folder_path: str, output_path: str, output_file_name: str):
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

    if not os.path.exists(output_path):
      os.makedirs(output_path)

    output_file_path = os.path.join(output_path, output_file_name)

    big_df.to_parquet(output_file_path, index=False, compression='zstd')
    print(f"Data successfully saved to {output_file_name} with Zstandard compression.")