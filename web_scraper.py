import pandas as pd
import sqlite3
import requests_html
from string import punctuation
import re
from sys import argv


def clear_name(string):
    string = string.replace('_', '-')
    string = string.replace(' ', '-')
    punc = punctuation.replace("-", "")
    punc = punc.replace("!", "")
    r = re.compile(r'([.,/#!$%^&*;:{}=_`~()-])[.,/#!$%^&*;:{}=_`~()-]+')
    return r.sub(r'\1', string.translate(str.maketrans("", "", punc)).lower())

    



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
        try:
            with open('errors_reviews.txt', 'a') as f:
                f.writelines('\n\n'.join(data) + "\n")
        except:
            return ""
        return ""
    
def thread_func(data):
    return data.apply(find_reviews, axis=1)



if __name__ == "__main__":

    conn = sqlite3.connect("data/preprocessed_dataset.db")
    dataset = pd.read_sql("SELECT * FROM games", conn)
    conn.close()
    dataset.set_index("index", inplace=True)


    f = int(argv[1])
    s = int(argv[2])

    
    revs = thread_func(dataset.loc[f:s])
    print(revs.head())
    revs.to_csv(f"data/reviews{int(argv[3])}.csv")








