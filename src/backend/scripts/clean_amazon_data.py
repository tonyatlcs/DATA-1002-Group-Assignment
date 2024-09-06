import pandas as pd
import os
from typing import List
class Clean_amazon_data:
  def __init__(self):
    pass

  def merge_review_meta_data(self, meta_folder_path: str, review_folder_path: str):
    # Read the data from the review and meta data folder
    review_data_folder_path = review_folder_path
    meta_data_folder_path = meta_folder_path

    # Get the files in the review and meta data folder
    files_in_review_data_folder = set(os.listdir(review_data_folder_path))
    files_in_meta_data_folder = set(os.listdir(meta_data_folder_path))

    # Get the common files in the review and meta data folder
    common_files = files_in_review_data_folder.intersection(files_in_meta_data_folder)

    # Initialise scoped variables
    joined_df = pd.DataFrame()
    joined_df_array = []


    # Loop through the common files in the review and meta data folder
    for common_file in common_files:
      meta_file_path = os.path.join(meta_data_folder_path, common_file)
      review_file_path = os.path.join(review_data_folder_path, common_file)

      meta_df = pd.read_json(meta_file_path, lines=True)
      meta_df = meta_df.rename(
        columns = {
          'title': 'meta_title',
        }
      )
      review_df = pd.read_json(review_file_path, lines=True)

      # Merge the meta and review dataframes
      joined_df = pd.merge(meta_df, review_df, on='parent_asin', how='outer')

      # Take a random 10000 rows from the joined_df
      joined_df = joined_df.sample(n=10000, random_state = 1)

      # Append the joined_df to the joined_df_array
      joined_df_array.append(joined_df)

    return joined_df_array

  def aggregate_json_data(self, joined_amazon_data_arr: List, output_path: str, output_file_name: str):
    aggregated_joined_df = pd.DataFrame()

    # Loop through the joined_amazon_data_arr and concatenate the dataframes
    for joined_df in joined_amazon_data_arr:
      aggregated_joined_df = pd.concat([aggregated_joined_df, joined_df])

    
    if not os.path.exists(output_path):
      os.makedirs(output_path)

    # Rename the columns
    aggregated_joined_df = aggregated_joined_df.rename(
      columns = {
        'user_id': 'user_id',
        'asin': 'product_id', 
        'title': 'product_name',
        'rating': 'rating',
        'timestamp': 'timestamp',
        'verified_purchase': 'has_purchased',
        'price': 'price',
        'meta_title': 'product_review_comment',
        'main_category': 'category',
      }
    )
    
    # define the columns that are needed for the cleaned dataset
    cleaned_big_df = aggregated_joined_df[
      [
        'user_id',
        'product_id',
        'product_name',
        'rating',
        'timestamp',
        'has_purchased',
        'price',
        'product_review_comment',
        'description',
        'category'
      ]
    ]

    # clean any unreadable values in the "price" column
    cleaned_big_df['price'] = pd.to_numeric(cleaned_big_df['price'], errors='coerce')

    # Turn cleaned_big_df into a excel file
    cleaned_big_df.to_csv('cleaned_big_df.csv', index=False)
    output_file_path = os.path.join(output_path, output_file_name)

    # Save the cleaned data to a parquet file
    cleaned_big_df.to_parquet(output_file_path, index=False, compression='zstd')
    print(f"Data successfully saved to {output_file_name} with Zstandard compression.")