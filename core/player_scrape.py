import pandas as pd
import requests
from bs4 import BeautifulSoup
from abc import ABC



def to_numeric(df):
    """This should be split up so that it can take column names as arguments to make it more pythonic. Will most likely
    make this a static method of the Athlete class at some point."""
    columns = list(df.columns)
    columns_to_numeric = columns[0:-2]
    final_cols = columns_to_numeric[0:2]+columns_to_numeric[4:]
    df[final_cols] = df[final_cols].apply(pd.to_numeric)
    return df


def get_soup(url):

    """This function is just to reduce redundancy. It gets the soup with html parser functionality for a given url"""
    website = requests.get(url)
    content = website.content
    soup = BeautifulSoup(content,features="html.parser")
    return soup


class Athlete(ABC):

    def __init__(self, name, suffix):
        if suffix != "01":  # Used to identify multiple players with the same name in the url
            self.name = name + " " + suffix
        else:
            self.name = name
        self.suffix = suffix
        self.first_name = name.split(" ")[0]
        self.last_name = name.split(" ")[1]

    def get_table(self, soup, table_id, num_columns, classes=[], outer_level=0):
        """Core functionality for all child classes. This function will the only web scraping funciton that will be
        called automatically at the creation of each instance of the child classes.

        It will be used to find the annual career statistics for each player and will return it in thw form of a pandas
        DataFrame.

        ---Arguments---

        -soup:          bs4 instance. un-parsed html from each player's web page
        -table_id:      String variable. This will identify the type of table to grab based on the position each athlete
                        plays in their respective sport
        -num_columns:   specifies the number of columns each table will have based on the type of player and data table
        -classes:       List variable. Some tables have different 'tr' classes based on mahor and minor leagues. This will
                        specify which one should be grabbed.
        """
        table = soup.find('table', attrs={"id": table_id})

        # Finds the "Career Statistics" Table
        columns = table.find_all('th')

        all_headings = []
        for entry in columns:
            all_headings.append(entry.get_text())  # Gets the heading for each column
            # Will be used to create the dataframe

        column_headings = all_headings[outer_level:num_columns]

        majors_table_rows = table.find_all('tr',  attrs={"class": classes})
        # selects the rows of the table

        row_data = []
        for tr in majors_table_rows:    # gets the data from each tr, essentially getting the stats for each year. Only gets major league data right now
            row = [td.get_text() for td in tr]  # creates a list of each data entry in the given table row
            row_data.append(row)

        pd_table = pd.DataFrame(row_data, columns=column_headings)


        names_list = [self.name]*len(row_data)
        pd_table["name"] = names_list #

        return pd_table

    def get_column_headings(self, cols, num_columns): #Gets the column heading from the columsn and the number of columns we want
        all_headings = []
        for entry in cols:
            all_headings.append(entry.get_text())  # Gets the heading for each column
            # Will be used to create the dataframe

        df_columns = all_headings[:num_columns]
        return df_columns


class MLB(Athlete):

    def __init__(self, name, init_stats=False, suffix="01"):
        super(MLB, self).__init__(name, suffix)
        self.url_last_name = self.last_name[0:5].lower() if len(self.last_name) >= 5 else self.last_name.lower()
        self.url_first_name = self.first_name[0:2].lower()
        self._base_url = "https://www.baseball-reference.com/players/{0}/{1}{2}.shtml"
        self.url = "https://www.baseball-reference.com/players/{0}/{1}{2}.shtml".format(
                    self.url_last_name[0], self.url_last_name + self.url_first_name, self.suffix)
        self.init_stats = init_stats

        if self.init_stats:

            self.soup = get_soup(self.url)

            if self.soup.find_all('table')[0].get('id') == "batting_standard":
                self.id = "batting_standard"
                self.num_columns = 30
                self.career_stats = self.get_hitting()

            if self.soup.find_all('table')[0].get('id') == "pitching_standard":
                self.id = "pitching_standard"
                self.num_columns = 35
                self.career_stats = self.get_pitching()


    def get_hitting(self):
        return self.get_table(self.soup, "batting_standard", self.num_columns, classes=["full", ""])

    def get_pitching(self):
        return self.get_table(self.soup, "pitching_standard", self.num_columns, classes=["full", ""])

    def get_summary(self):
        table = self.soup.find('table', attrs={"id": self.id})
        # Finds the "Career Statistics" Table
        columns = table.find_all('th')

        all_headings = []
        for entry in columns:
            all_headings.append(entry.get_text())  # Gets the heading for each column
            # Will be used to create the dataframe

        column_headings = ["Summary"] + all_headings[4:self.num_columns]

        majors_table_rows = table.find('tfoot').find_all('tr')

        row_data = []
        for tr in majors_table_rows:    # gets the data from each tr, essentially getting the stats for each year. Only gets major league data right now
            row = [td.get_text() for td in tr]  # creates a list of each data entry in the given table row
            row_data.append(row)

        pd_table = pd.DataFrame(row_data, columns=column_headings)


        names_list = [self.name]*len(row_data)
        pd_table["name"] = names_list

        return pd_table




class NFL(Athlete):
    """Not active"""
    table_names = {"WR": "receiving_and_rushing"}

    def __init__(self, name, init_stats=False, suffix="01", player_url="", ):
        super(NFL, self).__init__(name, suffix)
        self.url_last_name = self.last_name[0].upper() + self.last_name[1:4].lower() if len(self.last_name) >= 5 \
            else self.last_name[0].upper()+ self.last_name[1:].lower()

        self._base_url = "https://www.pro-football-reference.com"
        self.url = self._base_url.format(self.url_last_name[0]) + player_url
        self.num_columns = 26
        self.init_stats = init_stats
        self.soup = get_soup(self.url)

        if self.init_stats:

            self.id = "receiver"
            self.career_stats = self.get_receiver_stats()
            self.career_summary = self.get_summary(self.id)

    def get_summary(self, position, HOF):
        table = self.soup.find('table', attrs={"id": NFL.table_names[position]})

        # Finds the "Career Statistics" Table
        columns = table.find_all('th')

        all_headings = []
        for entry in columns:
            all_headings.append(entry.get_text())  # Gets the heading for each column
            # Will be used to create the dataframe

        column_headings = all_headings[9:21]
        majors_table_rows = table.find('tfoot').find_all('tr')

        row_data = []
        for tr in majors_table_rows:   # gets the data from each tr, essentially getting the stats for each year. Only gets major league data right now
            row = [td.get_text() for td in tr]  # creates a list of each data entry in the given table row
            row_data.append(row)

        final_row = row_data[0][5:17] + [self.name] + [HOF]

        return final_row

    def get_receiver_stats(self):
        return self.get_table(self.soup, "receiving_and_rushing", self.num_columns, classes=["full_table", ""], outer_level=8)

#davante = NFL("Davante Adams", player_url= "/players/A/AdamDa01.htm")
#table = davante.get_summary("WR", "No")
#print(table)


########### Below is currently used in the other modules#########





def get_player_data_pandas(name, sport="baseball", return_list=True):

    """Currently supplies the functionality to the other modules. Will eventually be phased out and functionality will
    be taken over by the 'Athlete' family of classes

    """
    sport_urls = {"football": "https://www.football-reference.com/players/{0}/{1}01.shtml",
                  "baseball":"https://www.baseball-reference.com/players/{0}/{1}01.shtml"}
    try:
        url = sport_urls[sport]
    except KeyError:
        return "That sport is not currently supported"   ###cheking to see that it is scraping form a valid source

    split_name = name.split(" ")
    first, last = split_name[0].lower(), split_name[1].lower()
    url_1 = last[0]
    url_2 = last[0:5] + first[0:2]

    full_url = url.format(url_1, url_2) ### Creates a specific URL based on the player's name
    ##this is baseball specific. We are finding the url based on the player's name

    soup = get_soup(full_url)
    ### Gets the soup for the given player.

    try:
        table = soup.find_all('table')[0]  # Finds the "Career Statistics" Table
        columns = table.find_all('th')

        all_headings = []
        for entry in columns:
            all_headings.append(entry.get_text())  # Gets the heading for each column
                                                   # Will be used to create the dataframe

        df_columns = all_headings[0:30]

        if table.get('id') != "batting_standard":  # Exception handling for if the the player us a pitcher
            return "Pitcher"  # ***ADD FUNCTIONALITY FOR PITCHERS***

    except IndexError:
        return "Error"

    table_rows = table.find_all('tr',  attrs={"class":["full", ""]})  #Finds the rows that are demarcated with the "full""
    # or "" class. this is to filter out minor league statistics. This is another oppurtunity for expanding functionality

    total_rows = []  # this will collect all of the row data in the form of lists

    for tr in table_rows: #gets the data from each tr, essentially getting the stats for each year
        row = [td.get_text() for td in tr]  # creates a list of each data entry in the given table row
        total_rows.append(row)

    pd_table = pd.DataFrame(total_rows, columns=df_columns)

    pd_table = to_numeric(pd_table)

    names_list = [name]*len(total_rows)
    pd_table["name"] = names_list  # this simply adds a column to the dataframe of the name of the player
    # This handles the issue of creating a dataframe that has the annual statistics of multiple players

    return pd_table




