import pandas as pd
import sqlite3
import numpy as np
import requests_html
from string import punctuation


conn = sqlite3.connect("preprocessed_dataset.db")
name_plat = pd.read_sql("SELECT name, platform FROM games", conn)
errors = []

def clear_name(string):
    string = string.replace('_', '-')
    string = string.replace(' ', '-')
    punc = punctuation.replace("-", "")
    punc = punc.replace("!", "")
    return string.translate(str.maketrans("", "", punc)).lower()


def find_reviews(data):
    
    session = requests_html.HTMLSession()
    print(f"https://www.metacritic.com/game/{clear_name(data['platform'])}/{clear_name(data['name'])}")
    try:
        
        r = session.get(f"https://www.metacritic.com/game/{clear_name(data['platform'][1:])}/{clear_name(data['name'])}")

        reviews_html = r.html.find(".critic_reviews")[0]
        reviews = "\n".join([review.text for review in reviews_html.find(".review_body")])
        print(data)
        return reviews
    except:
        with open('errors_reviews.txt', 'a') as f:
            f.writelines('\n\n'.join(data))
        return ""

name_plat["reviews"] = name_plat.apply(find_reviews, axis=1)
print(name_plat.head())
name_plat.to_csv("reviews.csv")





