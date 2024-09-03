from scripts.read import Read
from scripts.clean_amazon_data import Clean_amazon_data
from scripts.analyse_amazon_data import Analyse_amazon_data
import os

if __name__ == "__main__":
  clean_amazon_data = Clean_amazon_data()
  read_obj = Read()
  analyse_amazon_data = Analyse_amazon_data()

  amazon_review_path = '../../src/data/raw_data/amazon_data/review'
  amazon_output_path = '../../src/data/compressed/amazon_data'
  amazon_output_compressed_file_name = 'amazon_review_data.parquet'
  if not os.path.exists(amazon_output_path):
    clean_amazon_data.aggregate_json_data(amazon_review_path, amazon_output_path, amazon_output_compressed_file_name)

  # Reading amazon combined data
  amazon_combined_data_path = '../../src/data/compressed/amazon_data/amazon_review_data.parquet'
  amazon_data_df = read_obj.read_parquet_file(amazon_combined_data_path)
  rating_df = analyse_amazon_data.extract_title_ratings(amazon_data_df)
  print(rating_df)
  analyse_amazon_data.plot_ratings_distribution(rating_df)

  conversion_rate = analyse_amazon_data.caclulate_conversion_rate(amazon_data_df)
  print(f"Conversion rate: {conversion_rate}")
  analyse_amazon_data.plot_conversion_rate(conversion_rate)


  