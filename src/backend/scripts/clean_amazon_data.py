import pandas as pd
import os

class Clean_amazon_data:
  def __init__(self):
    pass

  def aggregate_json_data(self, meta_folder_path: str, review_folder_path: str, output_path: str, output_file_name: str):
    review_data_folder_path = review_folder_path
    meta_data_folder_path = meta_folder_path

    files_in_review_data_folder = set(os.listdir(review_data_folder_path))
    files_in_meta_data_folder = set(os.listdir(meta_data_folder_path))

    common_files = files_in_review_data_folder.intersection(files_in_meta_data_folder)
    joined_df = pd.DataFrame()

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

      joined_df = pd.merge(meta_df, review_df, on='parent_asin', how='inner')
    
    if not os.path.exists(output_path):
      os.makedirs(output_path)

    # I want to extract the user_id, asin, title, rating, timestamp, verified_purchase from the big_df into a new df with associated data and rename them to user_id, product_id, product_name, rating, timestamp, has_purchased
    joined_df = joined_df.rename(
      columns = {
        'user_id': 'user_id',
        'asin': 'product_id',
        'meta_title': 'product_name',
        'rating': 'rating',
        'timestamp': 'timestamp',
        'verified_purchase': 'has_purchased',
        'price': 'price',
        'title': 'product_review_comment',
        'main_category': 'category',
      }
    )
    
    # I want to make a new df with the columns user_id, product_id, product_name, rating, timestamp, has_purchased
    cleaned_big_df = joined_df[
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

    cleaned_big_df = cleaned_big_df[cleaned_big_df['description'].apply(lambda x: len(x) > 0)]

    cleaned_big_df = cleaned_big_df.sample(10000)

    cleaned_big_df = cleaned_big_df.dropna()

    output_file_path = os.path.join(output_path, output_file_name)

    cleaned_big_df.to_parquet(output_file_path, index=False, compression='zstd')
    print(f"Data successfully saved to {output_file_name} with Zstandard compression.")