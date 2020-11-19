from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, select, insert, update
from read_from_db import read_all_from_db, RosterConnection
from player_scrape import get_player_data_pandas, NFL
import pandas as pd
import sqlite3
import os



def mlb_add_to_all_players_db(db="sqlite", abs_path="/core/Databases/MLB/all_players.db", table="all_players_table",
                              row_vals=[]):
    """Adds players to database. Creates table and db and table if none exists. By default, the new databases will be
    created at /Databases/all_players.db from the current working directory. As of now, this only works for sqlite
    """
    error = ""
    if db == "sqlite":
        if not os.path.isfile(abs_path):
            print("Creating new database!")

        sql_engine = create_engine(db + ":///" + abs_path)
        metadata = MetaData(sql_engine)
        if not sql_engine.has_table(table):
        # creates the following table if the table does not exist
            player_list = Table(table, metadata,
                                Column("Id", Integer, primary_key=True),
                                Column('Name', String),
                                Column('First', Integer),
                                Column('Last', Integer),
                                Column('HOF', String),)
            metadata.create_all()
            mlb_add_to_all_players_db(db=db, abs_path=abs_path, table=table, row_vals=row_vals)  # just a simple one-step recursion if the table doesn't exist
        else:
            player_list = Table(table, metadata, autoload=True)
            try:
                s = select(player_list).where(player_list.c.Id == row_vals[0])  # This line is to trigger the exception
                # handling. If the above statement fails, this means that the current ID has not been created.
                # Therefore, a new one must be inserted rather than updating the current ID. The reason for doing it
                # this way is to account for when new players enter the league. This means that each time the table is
                # updated the indexes of players may change depending on if new have entered the league since the last
                # update. I will most likely think of a more sleek way to do this in the future.
                updt = update(player_list).where(player_list.c.Id == row_vals[0]).values(Name=row_vals[1],First=row_vals[2],
                                                                            Last=row_vals[3], HOF=row_vals[4])

                sql_engine.execute(updt)
            except:
                # Inserts the data into the table if the id does not already exist
                try:
                    ins = player_list.insert().values(Id=row_vals[0],Name=row_vals[1], First=row_vals[2], Last=row_vals[3], HOF=row_vals[4])
                    sql_engine.execute(ins)
                except:
                    print("Insert Failed") # This will be used to return the players that were not able to be processed
            sql_engine.dispose()


def mlb_create_annual_db(db="sqlite", abs_path="/core/Databases/MLB/NFL.db", source_table="all_players_table",
                         destination_table="all_players_batting", begin=0, end=1000):
    """I had initially tried to add the annual batting data to the sql table one row at a time but found this much too
    time consuming
    """

    if db == "sqlite":

        if not os.path.isfile(abs_path):
            print("Creating new database!")
        sql_engine = create_engine(db + ":////" + abs_path)
        metadata = MetaData(sql_engine)
        player_data = Table(destination_table, metadata, autoload=True)
        player_data.drop(sql_engine)


        try:
            df = read_all_from_db(table=source_table)
        except:
            return print("Please configure database before proceeding. This can be done with add_to_all_players_db(). "
                         "This will create a local sqlite db in the current working directory")

        players = list(df['Name'])[begin:begin+end]
        new_start = begin + end
        # This will use the code from player_scrape.py to return a list of pandas dataframes containing all Standard
        # Batting data available for the player's entire mlb career.
        aggregate_df = []

        for player in players:
            try:
                player_df = get_player_data_pandas(player)  #Returns a pandas dataframe of all of the annual batting data
            # for players

                if isinstance(player_df, pd.DataFrame):
                    player_df.to_sql(destination_table, con=sql_engine, if_exists="append")
                else:
                    continue
            except:
                continue

        sql_engine.dispose()




### NFL


def nfl_add_to_all_players_db(db="sqlite", abs_path="/core/Databases/NFL/NFL.db", table="all_nfl_players_table", row_vals=[]):
    """Adds players to database. Creates table and db and table if none exists. By default, the new databases will be
    created at /Databases/all_players.db from the current working directory. As of now, this only works for sqlite
    """
    error = ""
    if db == "sqlite":
        if not os.path.isfile(abs_path):
            print("Creating new database!")

        sql_engine = create_engine(db + ":///" + abs_path)
        metadata = MetaData(sql_engine)
        if not sql_engine.has_table(table):
            # creates the following table if the table does not exist
            player_list = Table(table, metadata,
                                Column("Id", Integer, primary_key=True),
                                Column('Position', String),
                                Column('Name', String),
                                Column('First', Integer),
                                Column('Last', Integer),
                                Column('HOF', String),
                                Column('HREF', String))
            metadata.create_all()
            nfl_add_to_all_players_db(db=db, abs_path=abs_path, table=table, row_vals=row_vals)  # just a simple one-step recursion if the table doesn't exist
        else:
            player_list = Table(table, metadata, autoload=True)
            try:
                s = select(player_list).where(player_list.c.Id == row_vals[0])  # This line is to trigger the exception
                # handling. If the above statement fails, this means that the current ID has not been created.
                # Therefore, a new one must be inserted rather than updating the current ID. The reason for doing it
                # this way is to account for when new players enter the league. This means that each time the table is
                # updated the indexes of players may change depending on if new have entered the league since the last
                # update. I will most likely think of a more sleek way to do this in the future.
                updt = update(player_list).where(player_list.c.Id == row_vals[0]).values(Position=row_vals[1], Name=row_vals[2],
                                                                                         First=row_vals[3],
                                                                                         Last=row_vals[4], HOF=row_vals[5], HREF=row_vals[6])

                sql_engine.execute(updt)
            except:
                # Inserts the data into the table if the id does not already exist
                try:
                    ins = player_list.insert().values(Id=row_vals[0], Position=row_vals[1], Name=row_vals[2], First=row_vals[3],
                                                      Last=row_vals[4], HOF=row_vals[5], HREF=row_vals[6])
                    sql_engine.execute(ins)
                except:
                    print("Insert Failed") # This will be used to return the players that were not able to be processed
            sql_engine.dispose()


def nfl_stat_builder(db="sqlite", database="", destination_table="nfl_wrs_1994", position="", last_year=0, HOF=True):
    """I had initially tried to add the annual batting data to the sql table one row at a time but found this much too
    time consuming
    """

    if db == "sqlite":

        if not os.path.isfile(database):
            print("Creating new database!")
        sql_engine = create_engine(db + ":///" + database)

        nfl_players = RosterConnection(db=database, table="all_nfl_players_table")

        df = nfl_players.special_select(column="Position", value=position, last_year=last_year)

        players = list(df['Name'])
        print(players)
        player_url = list(df['HREF'])
        HOF = list(df['HOF'])

        # This will use the code from player_scrape.py to return a list of pandas dataframes containing all Standard
        # Batting data available for the player's entire mlb career.
        i = 0
        errors = []

        while i < len(players):

            player = NFL(players[i], player_url=player_url[i])  #Returns a pandas dataframe of all of the annual batting data
            try:
                player_data = player.get_summary(position, HOF[i])
                player_df = pd.DataFrame([player_data], columns=['GS', 'Tgt', 'Rec', 'Yds', 'Y/R', 'TD', '1D', 'Lng', 'R/G', 'Y/G', 'Ctch%',
                                                            'Y/Tgt', 'Name', 'HOF'])
                if isinstance(player_df, pd.DataFrame):
                    player_df.to_sql(destination_table, con=sql_engine, if_exists="append")
                    print(player.name)

            except:
                errors.append(player.name)
                print("error ", player.name)
            i += 1

        sql_engine.dispose()

        return errors




#errors = nfl_stat_builder(database="/Users/nickblackmore/personal_projects/sportscrape/sportscrape/core/Databases/NFL.db", position='WR', last_year=1994)

def get_annual_stats(db="sqlite", database="", destination_table="nfl_wrs_1994_annual", position="", last_year=0):
    if not os.path.isfile(database):
        print("Creating new database!")
    sql_engine = create_engine(db + ":///" + database)

    nfl_players = RosterConnection(db=database, table="all_nfl_players_table")

    df = nfl_players.special_select(column="Position", value=position, last_year=last_year)

    players = list(df['Name'])
    print(players)
    player_url = list(df['HREF'])


    # This will use the code from player_scrape.py to return a list of pandas dataframes containing all Standard
    # Batting data available for the player's entire mlb career.
    i = 0
    errors = []

    while i < len(players):

        player = NFL(players[i], player_url=player_url[i])  #Returns a pandas dataframe of all of the annual batting data
        try:
            player_data = player.get_receiver_stats()
            player_df = pd.DataFrame(player_data, columns=['Year', 'Age', 'Tm', 'Pos', 'No.', 'G', 'GS', 'Tgt', 'Rec', 'Yds', 'Y/R', 'TD', '1D', 'Lng', 'R/G',
                                                             'Y/G',  'Ctch%', 'Y/Tgt', 'name'
                                                           ])
            print('success')
            if isinstance(player_df, pd.DataFrame):
                player_df.to_sql(destination_table, con=sql_engine, if_exists="append")
                print(player.name)

        except:
            errors.append(player.name)
            print("error ", player.name)

        i += 1

    sql_engine.dispose()

    return errors

errors = get_annual_stats(database="/Users/nickblackmore/personal_projects/sportscrape/sportscrape/core/Databases/NFL.db", position='WR', last_year=1994)

"""
Database schema:

NFL.db
    -- all_nfl_players_table (
    -- nfl_wrs_1994_annual (Year by Year Stats for wide receivers)
    -- nfl_wrs_1994





"""

def delete_sqlite_table(db, table=''):

    conn = sqlite3.connect(db)
    cur = conn.cursor()
    sql = "DELETE FROM {0}".format(table)
    cur.execute(sql)
    conn.commit()


#delete_sqlite_table("/Users/nickblackmore/personal_projects/sportscrape/sportscrape/core/Databases/NFL.db",
                    #table="nfl_wrs_1994_annual")