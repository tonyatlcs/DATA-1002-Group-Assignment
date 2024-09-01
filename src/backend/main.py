from scripts.read import Read
from scripts.clean_amazon_data import Clean_amazon_data
import os

if __name__ == "__main__":
  clean_amazon_data = Clean_amazon_data()

  amazon_digital_music_path = '../../src/data/raw_data/amazon_data/review/'

  clean_amazon_data.aggregate_review_data(amazon_digital_music_path)
  