import argparse
import csv
import sqlite3
import requests
from abc import ABCMeta, abstractmethod
from re import sub
from decimal import Decimal


class DBConfig:
    """
    SQL Database management
    """

    def __init__(self, db_name):

        # DB Config stuff
        self.conn = sqlite3.connect(db_name)
        self.conn.row_factory = sqlite3.Row  # Accessible object instead of plain tuple
        self.c = self.conn.cursor()

        # DB Tables
        self.movies_table = 'MOVIES'

    def execute_statement(self, sql_statement):
        """
        Execute the given statement
        :param sql_statement:
        :return:
        """

        # Statement execution
        try:
            self.c.execute(sql_statement)
        except sqlite3.OperationalError:
            # Not existing column
            raise
        results = self.c.fetchall()

        return results

    def update_data(self, downloaded_results):
        """
        Inserting downloaded results to the database
        :param downloaded_results:
        :return:
        """

        for result in downloaded_results:

            try:
                # Converting money to integer
                money = result['BoxOffice']
                money_value = Decimal(sub(r'[^\d.]', '', money))
            except Exception:  # Tried to handle particular exception but can't catch it
                money_value = result['BoxOffice']

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
                     'boxoffice': str(money_value)  # Setting the converted value or 'N/A'
                     })

        # Changing all of N/A values to NULL because of further conveniance
        with self.conn:
            self.c.execute(f"""UPDATE {self.movies_table} SET box_office = NULL 
                                WHERE box_office = 'N/A';""")


class CommandHandler:
    """
    Interface to ensure every handler of specific command uses the method handle and has a keyword
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def handle(self, parameter):
        # Handling commands by specified classes
        raise NotImplementedError

    @abstractmethod
    def get_keyword(self):
        # Specific keyword to handle command required
        raise NotImplementedError

    @classmethod
    def print_results(cls, results):
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


class DataUpdater(CommandHandler):
    """
    Handling updating database
    """

    def __init__(self, db):
        self.keyword = 'update'  # Constant keyword to recognize handler
        self.db = db  # DB to update

        self.results = []  # List for downloaded json results
        self.api_key = '305043ae'  # static API key to use API

    @property
    def sql_statement(self):
        # Statement to execute - if director column is null than everything must be null
        sql_statement = f"""SELECT {self.db.movies_table}.title FROM {self.db.movies_table} 
                            WHERE director IS NULL"""
        return sql_statement

    def handle(self, parameter):
        """
        Handling the update command request
        :return:
        """

        # Update: True or False whether the user wanted the update
        if parameter:
            # Getting titles with empty data
            empty_titles = self.db.execute_statement(sql_statement=self.sql_statement)

            # Downloading data for empty titles using API
            api_data = self.download_data(empty_titles)

            # Updating database with downloaded data
            self.db.update_data(api_data)

    def download_data(self, titles):
        """
        Downloading data using API
        """

        for title in titles:
            payload = {'t': title, 'r': 'json'}  # Get full data based on title in json format
            respond = requests.get(f'http://www.omdbapi.com/?apikey={self.api_key}', params=payload).json()
            self.results.append(respond)

        return self.results

    def get_keyword(self):
        return self.keyword


class DataSorter(CommandHandler):
    """
    Handling the sort command request
    """

    def __init__(self, db):
        self.keyword = 'sort_by'
        self.db = db  # DB to update
        self.parameter = None

    @property
    def sql_statement(self):
        # Statement to execute - sorting
        sql_statement = f"""SELECT {self.db.movies_table}.title, {self.db.movies_table}.{self.parameter}
                            FROM {self.db.movies_table}
                            ORDER BY {self.parameter} DESC"""
        return sql_statement

    def handle(self, parameter):
        # Setting the parameter
        self.parameter = parameter

        # Get the results from db
        results = self.db.execute_statement(self.sql_statement)

        # Print them out
        self.print_results(results)

        return results

    def get_keyword(self):
        return self.keyword


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
        parser = argparse.ArgumentParser(prog='my_program', description='Working with movies DB.')
        parser.add_argument('--version', action='version', version='1.0.0')

        # Updating records
        parser.add_argument('--update', help='update records', action='store_true')

        # Sorting records
        parser.add_argument('--sort_by', help='sort records', action='store', nargs=1,
                            choices=['id', 'title', 'year', 'runtime', 'genre',
                                     'director', 'cast', 'writer', 'language', 'country',
                                     'awards', 'imdb_rating', 'imdb_votes', 'box_office'],
                            type=str, default='id')

        args = parser.parse_args()

        commands = dict()
        commands['update'] = args.update
        commands['sort_by'] = args.sort_by[0]

        return commands


class Main:
    """
    Main program class
    """

    @staticmethod
    def main():
        """
        Main program function
        :return:
        """
        # Initialization of DB connection
        db = DBConfig(db_name='movies.sqlite')

        # Commands to handle
        handlers = [DataUpdater(db=db), DataSorter(db=db)]

        # Parse commands from args
        commands = CLInterface.get_args()

        # Performing commands
        Main.handle_commands(commands=commands, handlers=handlers)

    @staticmethod
    def handle_commands(commands, handlers):
        for key in commands.keys():
            for handler in handlers:
                if key == handler.get_keyword():
                    param = commands[key]
                    handler.handle(parameter=param)


if __name__ == '__main__':
    Main.main()

# THINGS TO BE REUSED IN THE FUTURE


# class FileParser:
#     """
#     Reading the list of titles from txt file
#     """
#
#     def __init__(self, titles_to_get):
#         self.titles_to_get = titles_to_get
#         self.titles = []
#
#     def read_titles(self):
#         """
#         Reading titles from a txt file
#         """
#
#         try:
#             # Read from file
#             with open(self.titles_to_get, 'r') as read_file:
#                 for line in read_file:
#                     line = line.rstrip('\n')
#                     self.titles.append(line)
#         except FileNotFoundError as e:
#             raise e


# def filter_data(self, parameter, value):
#     """
#     Returning data based on inserted title
#     :return:
#     """
#
#     # Statement to run
#     sql_statement = f"""SELECT {self.movies_table}.title, {self.movies_table}.{parameter}
#                         FROM {self.movies_table}
#                         WHERE {self.movies_table}.{parameter} LIKE '%{value}%';"""
#
#     # Statement execution
#     try:
#         self.c.execute(sql_statement)
#         result = self.c.fetchall()
#     except sqlite3.OperationalError:
#         # Not existing column
#         raise
#
#     return result
#
# def compare(self, parameter, titles_to_compare):
#     """
#     Comparing two titles
#     :param parameter:
#     :param titles_to_compare:
#     :return:
#     """
#
#     title_1 = titles_to_compare[0]
#     title_2 = titles_to_compare[1]
#
#     sql_statement = f"""SELECT {self.movies_table}.title, {self.movies_table}.{parameter}
#                         FROM {self.movies_table}
#                         WHERE {self.movies_table}.title IN ('{title_1}', '{title_2}')
#                         ORDER BY {self.movies_table}.{parameter} desc
#                         LIMIT 1"""
#
#     # Statement execution
#     try:
#         self.c.execute(sql_statement)
#         result = self.c.fetchall()
#     except sqlite3.OperationalError:
#         # Not existing column
#         raise
#
#     return result

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
