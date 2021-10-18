import pandas as pd
import sqlite3

all_games = pd.read_csv("all_games.csv")
conn = sqlite3.connect("dataset.db")

all_games.to_sql("games", con=conn)
conn.close()