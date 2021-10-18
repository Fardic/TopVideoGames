import pandas as pd
from sqlalchemy import create_engine
import sqlite3

all_games = pd.read_csv("all_games.csv")
conn = sqlite3.connect("dataset.db")
# engine = create_engine('sqlite:///dataset.db', echo=False)

all_games.to_sql("games", con=conn)
conn.close()