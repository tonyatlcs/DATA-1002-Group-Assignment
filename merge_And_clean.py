import pandas as pd
import os

folder_path = os.getcwd()

links_df = pd.DataFrame()
movies_df = pd.DataFrame()
ratings_df = pd.DataFrame()
tags_df = pd.DataFrame()

# loading files (had to use lowers as i was getting errors otherwise)
for file in os.listdir(folder_path):
    if file.endswith('.csv'):
        file_path = os.path.join(folder_path, file)
        if "links" in file.lower():
            links_df = pd.read_csv(file_path)
        elif "movies" in file.lower():
            movies_df = pd.read_csv(file_path)
        elif "ratings" in file.lower():
            ratings_df = pd.read_csv(file_path)
        elif "tags" in file.lower():
            tags_df = pd.read_csv(file_path)


# merging for movieID
movies_links_merged = pd.merge(movies_df, links_df, on='movieId', how='left')

# movies and links for ratings
ratings_merged = pd.merge(ratings_df, movies_links_merged, on='movieId', how='left')

# merging tags onto old df
tags_merged = pd.merge(tags_df, ratings_merged, on=['userId', 'movieId'], how='left')

# columns
tags_merged_cleaned = tags_merged.rename(columns={
    'userId': 'userID',
    'movieId': 'productID',
    'title': 'productName',
    'rating': 'rating',
    'timestamp_x': 'timestamp',
    'genres': 'category',
    'tag': 'Description',
    'timestamp_y': 'productReviewComment'
})

# null columns
tags_merged_cleaned['Price'] = None
tags_merged_cleaned['hasPurchased'] = None

# adding productreviewcomments with existing
tags_merged_cleaned['productReviewComment'] = tags_merged_cleaned.apply(
    lambda row: f"Tag: {row['Description']}, Rating: {row['rating']}" if pd.notnull(row['Description']) else f"Rating: {row['rating']}",
    axis=1
)

final_dataset = tags_merged_cleaned[['userID', 'productID', 'productName', 'rating', 'timestamp', 'hasPurchased', 'Price', 'productReviewComment', 'Description', 'category']]

# null for missing data
final_dataset_filled = final_dataset.fillna('Null')

# new csv
output_file = os.path.join(folder_path, 'final_cleaned_dataset.csv')
final_dataset_filled.to_csv(output_file, index=False)

