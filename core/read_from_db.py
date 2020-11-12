from sqlalchemy import create_engine
import pandas as pd
import sqlite3
from datetime import datetime


def read_all_from_db(db="sqlite", abs_path= "/core/Databases/NFL.db",
                     table="all_nfl_players_table"):
    """Reads data from a locally stored sqlite database. For the remainder of this module we will use the sqlite3 library
    as this is the only type of database supported at this time.
    """
    if not abs_path == "":
        sql_engine = create_engine(db + ":///" + abs_path)
    else:
        sql_engine = create_engine(db + ":///" + abs_path )
    query = "select * from {0}".format(table)
    df = pd.read_sql_query(query, sql_engine)
    return df


def select_all_from_db(table, db_file):

    conn = sqlite3.connect(db_file)
    df = pd.read_sql_query("SELECT * FROM {0}".format(table), conn)
    return df


class RosterConnection():


    def __init__(self, db, table):

        self.db = db
        self.connection = sqlite3.connect(db)
        self.table = table

    def select_all(self):
        return select_all_from_db(self.table, self.db)

    def special_select(self, column="", value="", first_year=None, last_year=None, HOF=False):

        df = pd.read_sql_query("SELECT * FROM {0} WHERE {1} == '{2}' AND First >= {3} AND Last >= {4} ".format(
                self.table, column, value, "0" if first_year is None else first_year, str(datetime.now().year) if
                last_year is None else last_year), con=self.connection)

        return df




#x = read_all_from_db(abs_path= "/Users/nickblackmore/personal_projects/sportscrape/sportscrape/core/Databases/NFL.db")
#print(x[x['Name'] == 'Calvin Johnson '])


#Example:

#nfl_players = RosterConnection(db="/Users/nickblackmore/personal_projects/sportscrape/sportscrape/core/Databases/NFL.db",
                               #table="all_nfl_players_table")


#df = nfl_players.special_select(column="Position", value="WR")

#print(df.head())




