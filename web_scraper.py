import pandas as pd
import sqlite3
import numpy as np
import requests_html
from string import punctuation
import threading
import concurrent.futures


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
            f.writelines('\n\n'.join(data) + "\n")
        return ""
    
def thread_func(data):
    return data.apply(find_reviews, axis=1)



if __name__ == "__main__":

    conn = sqlite3.connect("data/preprocessed_dataset.db")
    dataset = pd.read_sql("SELECT * FROM games", conn)
    conn.close()
    dataset.set_index("index", inplace=True)


    with concurrent.futures.ThreadPoolExecutor() as executor:
        future1 = executor.submit(thread_func, dataset.loc[:470])
        future2 = executor.submit(thread_func, dataset.loc[471:941])
        future3 = executor.submit(thread_func, dataset.loc[942:1412])
        future4 = executor.submit(thread_func, dataset.loc[1413:])

        
    revs = pd.concat([future1.result(), future2.result(), future3.result(), future4.result()])
    print(revs.head())
    revs.to_csv("data/reviews.csv")

    dataset["reviews"] = revs
    dataset.to_csv("data/dataset_reviews.csv")

    conn = sqlite3.connect("data/dataset_reviews.db")
    dataset.to_sql("games", conn)
    conn.close()








