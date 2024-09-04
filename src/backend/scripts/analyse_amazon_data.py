import numpy as np
import matplotlib.pyplot as plt

class Analyse_amazon_data:
  def __init__(self):
    pass

  def extract_title_ratings(self, df):
    df_name_rating = df[['product_name', 'rating', 'has_purchased']]
    return df_name_rating
  
  def caclulate_conversion_rate(self, df):
    verified_purchase = df['has_purchased'].to_numpy()
    print(verified_purchase)
    conversion_rate = np.sum(verified_purchase) / len(verified_purchase)
    return conversion_rate

  def plot_ratings_distribution(self, df):
    ratings_arr = df['rating'].to_numpy()
    print(ratings_arr)

    plt.hist(ratings_arr, bins=10, edgecolor='black')
    plt.title('Ratings Distribution')
    plt.xlabel('Ratings')
    plt.ylabel('Frequency')
    plt.show()
  
  


  

    