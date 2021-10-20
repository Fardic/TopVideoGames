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


session = requests_html.HTMLSession()
r = session.get(f"https://www.metacritic.com/game/nintendo-64/the-legend-of-zelda-ocarina-of-time")











def find_years(data):
    
    session = requests_html.HTMLSession()
    print(f"https://www.metacritic.com/game/{clear_name(data['platform'])}/{clear_name(data['name'])}")
    try:
        
        r = session.get(f"https://www.metacritic.com/game/{clear_name(data['platform'][1:])}/{clear_name(data['name'])}")

        summary_details = r.html.find(".summary_details", first=True)
        date = summary_details.find(".release_data", first=True).text

        print(data)
        return int(date[-4:])
    except:
        with open('errors_year.txt', 'a') as f:
            f.writelines('\n\n'.join(data))
        return -1000

#name_plat["release_year"] = name_plat.apply(find_years, axis=1)
#print(name_plat.head())
#name_plat.to_csv("years.csv")





