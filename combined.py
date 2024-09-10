import pandas as pd
import os

folder_path = os.getcwd()

files_to_combine = ["cleaned_data2.csv", "cleaned_big_df.csv", "final_cleaned_dataset.csv"]

df_list = []

for file_name in files_to_combine:
    file_path = os.path.join(folder_path, file_name)
    df = pd.read_csv(file_path)
    df_list.append(df)

combined_df = pd.concat(df_list, ignore_index=True)

output_file = os.path.join(folder_path, 'combined_dataset.csv')
combined_df.to_csv(output_file, index=False)

