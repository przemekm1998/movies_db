import argparse
import csv
import sqlite3
import requests
from decimal import Decimal
from re import sub


class FileParser:
    """
    Reading the list of titles from txt file
    """

    def __init__(self, titles_to_get):
        self.titles_to_get = titles_to_get
        self.titles = []

    def read_titles(self):
        """
        Reading titles from a txt file
        """

        try:
            # Read from file
            with open(self.titles_to_get, 'r') as read_file:
                for line in read_file:
                    line = line.rstrip('\n')
                    self.titles.append(line)
        except FileNotFoundError as e:
            raise e


class DBConfig:
    """
    SQL Database managements
    """

    def __init__(self, db_name):

        # DB Config stuff
        self.conn = sqlite3.connect(db_name)
        self.conn.row_factory = sqlite3.Row  # Accessible object instead of plain tuple
        self.c = self.conn.cursor()

        # DB Tables
        self.movies_table = 'MOVIES'

    def get_empty_titles(self):
        """
        Get empty titles to update data for them
        :return:
        """

        # Statement to execute - if director column is null than everything must be null
        sql_statement = f"""SELECT {self.movies_table}.title FROM {self.movies_table} 
                            WHERE director IS NULL"""

        self.c.execute(sql_statement)
        titles = self.c.fetchall()

        return titles

    def download_data(self, titles):
        """
        Updating information about movies' titles
        """

        API_KEY = '305043ae'  # API key to use API
        results = list()  # List of json results

        for title in titles:
            payload = {'t': title, 'r': 'json'}  # Get full data based on title in json format
            respond = requests.get(f'http://www.omdbapi.com/?apikey={API_KEY}', params=payload).json()
            results.append(respond)

        return results

    # def db_create(self):
    #     """
    #     Create tables if don't exist
    #     """
    #
    #     try:
    #         # Try to create table
    #         with self.conn:
    #             self.c.execute(f"""
    #                 CREATE TABLE {self.movies_table} (
    #                 title TEXT,
    #                 year INTEGER,
    #                 rated TEXT,
    #                 released TEXT,
    #                 runtime TEXT,
    #                 genre TEXT,
    #                 director TEXT,
    #                 writer TEXT,
    #                 actors TEXT,
    #                 plot TEXT,
    #                 language TEXT,
    #                 country TEXT,
    #                 awards TEXT,
    #                 poster TEXT,
    #                 metascore INTEGER,
    #                 imdbRating REAL,
    #                 imdbVotes INTEGER,
    #                 imdbID INTEGER,
    #                 type TEXT,
    #                 DVD TEXT,
    #                 boxoffice REAL,
    #                 production TEXT,
    #                 website TEXT,
    #                 PRIMARY KEY (title, year)
    #                 );
    #             """)
    #     except sqlite3.OperationalError:
    #         print(f'Database with table {self.movies_table} exists.')
    #         raise

    def update_data(self, downloaded_results):
        """
        Inserting downloaded results to the database
        :param downloaded_results:
        :return:
        """

        for result in downloaded_results:

            # Converting money to integer
            money = result['BoxOffice']
            try:
                money_value = int(Decimal(sub(r'[^\d.]', '', money)))
            except Exception:
                money_value = money

            with self.conn:
                self.c.execute(
                    f"""UPDATE {self.movies_table} 
                        SET year = :year,
                        runtime = :runtime,
                        genre = :genre,
                        director = :director,
                        cast = :cast,
                        writer = :writer,
                        language = :language,
                        country = :country,
                        awards = :awards,
                        imdb_Rating = :imdbRating,
                        imdb_Votes = :imdbVotes,
                        box_office = :boxoffice
                        WHERE title = :title;""",
                    {'title': result['Title'],
                     'year': result['Year'],
                     'runtime': result['Runtime'],
                     'genre': result['Genre'],
                     'director': result['Director'],
                     'writer': result['Writer'],
                     'cast': result['Actors'],
                     'language': result['Language'],
                     'country': result['Country'],
                     'awards': result['Awards'],
                     'imdbRating': result['imdbRating'],
                     'imdbVotes': result['imdbVotes'],
                     'boxoffice': money_value
                     })

    def filter_data(self, parameter, value):
        """
        Returning data based on inserted title
        :return:
        """

        # Statement to run
        sql_statement = f"""SELECT {self.movies_table}.title, {self.movies_table}.{parameter} 
                            FROM {self.movies_table}
                            WHERE {self.movies_table}.{parameter} LIKE '%{value}%';"""

        # Statement execution
        try:
            self.c.execute(sql_statement)
            result = self.c.fetchall()
        except sqlite3.OperationalError:
            # Not existing column
            raise

        self.print_results(results=result)

        return result

    def sort_data(self, parameter):
        """
        Return data sorted by parameter
        :param parameter:
        :return:
        """

        # Statement to run
        sql_statement = f"""SELECT {self.movies_table}.title, {self.movies_table}.{parameter}
                            FROM {self.movies_table}  
                            WHERE {self.movies_table}.{parameter} != 'N/A'
                            ORDER BY {parameter} DESC"""

        # Statement execution
        try:
            self.c.execute(sql_statement)
            result = self.c.fetchall()
        except sqlite3.OperationalError:
            # Not existing column
            raise

        self.print_results(results=result)

        return result

    def compare(self, parameter, titles_to_compare):
        """
        Comparing two titles
        :param parameter:
        :param titles_to_compare:
        :return:
        """

        title_1 = titles_to_compare[0]
        title_2 = titles_to_compare[1]

        sql_statement = f"""SELECT {self.movies_table}.title, {self.movies_table}.{parameter}
                            FROM {self.movies_table}
                            WHERE {self.movies_table}.title IN ('{title_1}', '{title_2}')
                            ORDER BY {self.movies_table}.{parameter} desc
                            LIMIT 1"""

        # Statement execution
        try:
            self.c.execute(sql_statement)
            result = self.c.fetchall()
        except sqlite3.OperationalError:
            # Not existing column
            raise

        self.print_results(results=result)

        return result

    def print_results(self, results):
        """
        Print output to console
        :param results:
        :return:
        """

        try:
            keys = results[0].keys()  # Loading keys
        except IndexError:
            # Empty results case
            raise

        # Printing keys at the top
        print('\n')  # Styling
        header = ""
        for key in keys:
            header += ('{:30}'.format(str(key)) + " | ")
        print(header)

        for res in results:
            ans = ""
            for key in res.keys():
                part = res[f'{key}']
                ans += ('{:30}'.format(str(part)) + " | ")
            print(ans)


class CSVWriter:
    """
    Creating csv file
    """

    @staticmethod
    def write_csv(title, data):
        """
        Saving results query as csv file
        :param data:
        :param title:
        :return:
        """

        with open(title, 'w') as csv_w:
            try:
                fieldnames = data[0].keys()  # Getting the csv fieldnames
            except IndexError:
                raise

            csv_writer = csv.DictWriter(csv_w, fieldnames=fieldnames, delimiter=',')
            csv_writer.writeheader()

            # Writing row in csv file for every result
            for result in data:
                row = dict(result)
                csv_writer.writerow(row)


class CLInterface:
    """
    Command Line Interface Class
    """

    @staticmethod
    def get_args():
        """
        Loading arguments given by user
        :return:
        """

        # Creating parser
        parser = argparse.ArgumentParser(description='Processing the commands.')

        # Adding arguments
        parser.add_argument('--update', metavar='update', type=str, help='update records')

        # Reading data from database and writing it to csv
        # parser.add_argument('--', metavar='print', type=str, help='print input',
        #                     default='eluwina', choices=['gitara', 'siema'])

        args = parser.parse_args()
        print(args.update)


if __name__ == "__main__":
    CLInterface.get_args()
