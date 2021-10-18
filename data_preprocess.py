import pandas as pd
import sqlite3
from sklearn.preprocessing import LabelBinarizer

conn = sqlite3.connect("dataset.db")
dataset = pd.read_sql("SELECT * FROM games", conn)
dataset.set_index("index", inplace=True)

conn.close()

# tbd values for user_review changed to -1
dataset.loc[dataset["user_review"] == "tbd", ["user_review"]] = -1
# dtypes are arranged
dataset.release_date = pd.to_datetime(dataset.release_date)
dataset.user_review = pd.to_numeric(dataset.user_review)
dataset.dtypes

# One hot encoding of platform column
ohe_platform = pd.get_dummies(dataset.platform, prefix='platform')
dataset = pd.concat([dataset, ohe_platform], axis=1)
del ohe_platform

# release date is seperated as publish_year and publish_month
dataset["publish_year"] = dataset["release_date"].apply(lambda x: int(x.year))
dataset["publish_month"] = dataset["release_date"].apply(lambda x: int(x.month))
dataset.drop(columns=["release_date"], inplace=True)

# Scaling for meta_score and user_reviews(user_reviews=-1 are excluded)
dataset["meta_score"] = dataset["meta_score"] / 100
dataset.loc[dataset["user_review"] != -1, ["user_review"]] = dataset.loc[dataset["user_review"] != -1, ["user_review"]] / 10

# Write preprocessed sql database
conn = sqlite3.connect("preprocessed_dataset.db")

dataset.to_sql("games", con=conn)
conn.close()