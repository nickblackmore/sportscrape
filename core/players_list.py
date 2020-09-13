import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import string
### This file will remain mostly the same during refactor

from add_rows_to_db import mlb_add_to_all_players_db, nfl_add_to_all_players_db


alphabet = list(string.ascii_lowercase)


def mlb_get_players_table_by_letter(letter):
    """This searches all of the players that have profiles on ProBaseballReference.com
    The full list of player, along with years they were active, will bes stored in the all_players_table. This can be used on
    its own or it can be used to further create databases i.e. for joins
    """


    url = "https://www.baseball-reference.com/players/{0}/".format(letter)

    website = requests.get(url)
    content = website.content
    soup = BeautifulSoup(content,features="html.parser")

    player_data_div = soup.find("div", {"id": "div_players_"})#Selects the div containing the list of all players
    return player_data_div



def mlb_get_player_list(letter, abs_path, output="db", start_index=0):
    """Fetches the list of players based on the starting letter of their last name. Users will have the option to
    output the data to a database or to pandas dataframe.
    """

    player_data_div = mlb_get_players_table_by_letter(letter)

    if output == "db":
        x = 0
        while x < len(player_data_div.find_all("p")): #each <p> tag corresponds to an individual player
            #the following code is going to parse the raw values taken from the html
            #yyyy-yyyy becomes yyyy and yyyy
            raw_year = player_data_div.find_all("p")[x].text[-10:-1]
            first_year = raw_year[0:4]
            last_year = raw_year[-4:]

            name = player_data_div.find_all("p")[x].find_all("a")[0].text  #name of the player
            HOF = "Yes" if "+" in player_data_div.find_all("p") else "No"  # whether the player is in the HOF

            data = [start_index, name, first_year, last_year, HOF] #corresponds with the row to be added to the db
            mlb_add_to_all_players_db(abs_path=abs_path, row_vals=data)
            x += 1
            start_index += 1
        return start_index

    elif output == "df":  # This will have the same functionality as above except it will return lists that are stored
        # in memory instead of being added to a table as rows

        raw_years = [p.text[-10:-1] for p in player_data_div.find_all("p")]
        HOF = []
        raw_players = []


        for p in player_data_div.find_all("p"):
            for player in p.find_all("a"):
                raw_players.append(player.text)


        for p in player_data_div.find_all("p"):
            #raw_years.append(p.text[-10:-1])
            if "+" in p.text:
                HOF.append("Yes")
            else:
                HOF.append("No")


        start_list = [start[0:4] for start in raw_years]
        end_list = [end[-4:] for end in raw_years]
        x = 0
        full_list = []

        while x < len(raw_players):

            full_list.append([raw_players[x],int(start_list[x]), int(end_list[x]), HOF[x]])
            x += 1

        return full_list


def get_all_mlb_players(abs_path, output='db'):
    """This will add all players alphabetically to the db or df
        It also has the control flow for maintaining index integrity
    """
    index = 0
    if output == "db":
        for letter in alphabet:
            end_index = mlb_get_player_list(letter, abs_path, start_index=index)
            index = end_index

    elif output == "df":
        full_list = []
        for letter in alphabet:
            full_list = full_list + mlb_get_player_list(letter)

        return pd.DataFrame(full_list, columns = ["Name", "Start", "End", "HOF?"])



def nfl_get_players_table_by_letter(letter):

    url = "https://www.pro-football-reference.com/players/{0}/".format(letter.upper())

    website = requests.get(url)
    content = website.content
    soup = BeautifulSoup(content,features="html.parser")

    player_data_div = soup.find("div", {"id": "div_players"})#Selects the div containing the list of all players
    return player_data_div


def nfl_get_player_list(letter, abs_path, output="db", start_index=0):
    """Fetches the list of players based on the starting letter of their last name. Users will have the option to
    output the data to a database or to pandas dataframe.
    """

    player_data_div = nfl_get_players_table_by_letter(letter)

    if output == "db":
        x = 0
        while x < len(player_data_div.find_all("p")): #each <p> tag corresponds to an individual player
            #the following code is going to parse the raw values taken from the html
            #yyyy-yyyy becomes yyyy and yyyy
            entry = player_data_div.find_all("p")[x].text
            raw_year = entry[-10:]
            first_year = raw_year[0:5]
            last_year = raw_year[-4:]

            try:  # regex to find the text from each <p> tag that is inside parentheses i.e. the position the player plays
                position = re.findall(r'\(([^)]+)\)', entry)[0]
            except IndexError:
                position = ""

            name = player_data_div.find_all("p")[x].find_all("a")[0].text  #name of the player
            HOF = "Yes" if "+" in entry else "No"
            url = player_data_div.find_all("p")[x].find_all('a', href=True)[0]['href'] # returns the href of each player


            data = [start_index, position, name, first_year, last_year, HOF, url] #corresponds with the row to be added to the db
            nfl_add_to_all_players_db(abs_path=abs_path, row_vals=data)

            x += 1
            start_index += 1
        return start_index

    elif output == "df":    # This will have the same functionality as above except it will return lists that are stored
                            # in memory instead of being added to a table as rows

        raw_years = [p.text[-10:-1] for p in player_data_div.find_all("p")]
        HOF = []
        raw_players = []


        for p in player_data_div.find_all("p"):
            for player in p.find_all("a"):
                raw_players.append(player.text)


        for p in player_data_div.find_all("p"):
            #raw_years.append(p.text[-10:-1])
            if "+" in p.text:
                HOF.append("Yes")
            else:
                HOF.append("No")


        start_list = [start[0:4] for start in raw_years]
        end_list = [end[-4:] for end in raw_years]
        x = 0
        full_list = []

        while x < len(raw_players):

            full_list.append([raw_players[x],int(start_list[x]), int(end_list[x]), HOF[x]])
            x += 1

        return full_list


def get_all_nfl_players(abs_path, output='db'):
    """This will add all players alphabetically to the db or df
        It also has the control flow for maintaining index integrity
    """
    index = 0
    if output == "db":
        for letter in alphabet:
            end_index = nfl_get_player_list(letter, abs_path, start_index=index)
            index = end_index

    elif output == "df":
        full_list = []
        for letter in alphabet:
            full_list = full_list + nfl_get_player_list(letter)

        return pd.DataFrame(full_list, columns = ["Name", "Position" "Start", "End", "HOF?", "HREF"])


#get_all_nfl_players(abs_path="/Users/nickblackmore/personal_projects/sportscrape/_database_creation/core/Databases/NFL.db")
