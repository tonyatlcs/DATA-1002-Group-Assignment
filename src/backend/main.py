from scripts.read import Read
from scripts.clean_amazon_data import Clean_amazon_data
import os

if __name__ == "__main__":
  clean_amazon_data = Clean_amazon_data()

  amazon_digital_music_path = '../../src/data/raw_data/amazon_data/review/'
  amazon_digital_music_output_path = '../../src/data/compressed/amazon_data'
  amazon_output_compressed_file_name = 'amazon_review_data.parquet'


  clean_amazon_data.aggregate_json_data(amazon_digital_music_path, amazon_digital_music_output_path, amazon_output_compressed_file_name)
  