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

    @staticmethod
    def value_updater(table):
        """
        Create converter for a given table
        :param table:
        :return:
        """
        return lambda column, value_to_change, new_value: f"""UPDATE {table} SET {column} = {new_value} 
                            WHERE {column} = {value_to_change};"""


class CommandHandler:
    """
    Interface to ensure every handler of specific command uses the method handle and has a keyword
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def handle(self, parameter):
        """
        The main handle function which all handlers must run
        :param parameter:
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def get_keyword(self):
        """
        Specific keyword of handler is required to identify it
        :return:
        """
        raise NotImplementedError


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
    def sql_empty_titles_statement(self):
        """
        Statement to execute - if director column is null than everything must be null
        :return:
        """
        sql_statement = f"""SELECT {self.db.movies_table}.title FROM {self.db.movies_table} 
                        WHERE director IS NULL"""
        return sql_statement

    @property
    def sql_data_update_statement(self):
        """
        Statement to execute - if director column is null than everything must be null
        :return:
        """
        sql_statement = f"""UPDATE {self.db.movies_table} 
                            SET year = :year, runtime = :runtime,
                                   genre = :genre, director = :director, writer = :writer,
                                   cast = :cast, language = :language,
                                   country = :country,
                                   awards = :awards, imdb_Rating = :imdbRating,
                                   imdb_Votes = :imdbVotes,
                                   box_office = :boxoffice
                            WHERE title = :title"""
        return sql_statement

    def handle(self, parameter):
        """
        Handling the update command request
        :return:
        """

        # Update: True or False whether the user wanted the update
        if parameter:
            # Getting titles with empty data
            empty_titles = self.db.execute_statement(sql_statement=self.sql_empty_titles_statement)
            empty_titles = [title['Title'] for title in empty_titles]  # Convrting SQL Row Objects to str

            # Downloading data for empty titles using API
            api_data = self.download_data(empty_titles)

            # Updating database with downloaded data
            self.update_data(api_data)

            # Changing box office N/A value to null for further conveniance
            self.na_to_null('box_office')

            return str(empty_titles) + " updated!"

    def download_data(self, titles):
        """
        Downloading data using API
        :param titles:
        :return:
        """

        for title in titles:
            payload = {'t': title, 'r': 'json'}  # Get full data based on title in json format
            respond = requests.get(f'http://www.omdbapi.com/?apikey={self.api_key}', params=payload).json()
            self.results.append(respond)

        return self.results

    def update_data(self, downloaded_results):
        """
        Updating the data in the database
        :param downloaded_results:
        :return:
        """

        def convert_money(money_to_convert):
            """
            Converting money to integer
            :param money_to_convert:
            :return:
            """
            try:
                money_int = Decimal(sub(r'[^\d.]', '', money_to_convert))
            except Exception:
                # Tried to handle particular exception but can't catch it
                return str(money_to_convert)

            return str(money_int)

        for result in downloaded_results:
            # Converting money
            money_str = result['BoxOffice']
            money_to_insert = convert_money(money_str)

            # Inserting data
            with self.db.conn:
                self.db.c.execute(self.sql_data_update_statement,
                                  {'title': result['Title'], 'year': result['Year'], 'runtime': result['Runtime'],
                                   'genre': result['Genre'], 'director': result['Director'], 'writer': result['Writer'],
                                   'cast': result['Actors'], 'language': result['Language'],
                                   'country': result['Country'], 'awards': result['Awards'],
                                   'imdbRating': result['imdbRating'], 'imdbVotes': result['imdbVotes'],
                                   'boxoffice': money_to_insert
                                   })

    def na_to_null(self, column):
        """
        Changing all of N/A values to NULL because of further conveniance
        :param column:
        :return:
        """

        na_to_null = DBConfig.value_updater(self.db.movies_table)
        sql_statement = na_to_null(column, "'N/A'", 'NULL')

        with self.db.conn:
            try:
                self.db.c.execute(sql_statement)
            except sqlite3.OperationalError:
                raise  # Not existing column

    def get_keyword(self):
        return self.keyword


class DataSorter(CommandHandler):
    """
    Handling the filter command request
    """

    def __init__(self, db):
        self.keyword = 'sort_by'
        self.db = db  # DB to update
        self.parameter = None
        self.value = None

    @property
    def sql_statement(self):
        """
        Statement to execute - sorting
        :return:
        """
        sql_statement = f"""SELECT {self.db.movies_table}.title, {self.db.movies_table}.{self.parameter}
                        FROM {self.db.movies_table}
                        ORDER BY {self.parameter} DESC"""
        return sql_statement

    def handle(self, parameter):
        """
        Handling the sorting command request
        :return:
        """

        # Setting the parameter
        self.parameter = parameter

        # Get the results from db
        try:
            results = self.db.execute_statement(self.sql_statement)
        except sqlite3.OperationalError as e:
            raise e

        # Return of the results
        return results

    def get_keyword(self):
        return self.keyword


class DataFilter(CommandHandler):
    """
    Handling the filtering command request
    """

    def __init__(self, db):
        self.keyword = 'filter_by'
        self.db = db

        self.column = None
        self.value = None

    def handle(self, parameter):
        """
        Handling the filtering request
        :param parameter:
        :return:
        """

        # Retreiving column name and value to filter by
        self.column = parameter[0]
        self.value = parameter[1]

        # Getting the results from the db
        try:
            results = self.db.execute_statement(self.sql_statement)
        except sqlite3.OperationalError as e:
            raise e

        # Return the results
        return results

    @property
    def sql_statement(self):
        """
        Statement to execute - sorting
        :return:
        """
        sql_statement = f"""SELECT {self.db.movies_table}.title, {self.db.movies_table}.{self.column}
                            FROM {self.db.movies_table}
                            WHERE {self.db.movies_table}.{self.column} LIKE '%{self.value}%';"""
        return sql_statement

    def get_keyword(self):
        return self.keyword


class DataCompare(CommandHandler):
    """
    Comparing two titles
    """

    def __init__(self, db):
        self.keyword = 'compare'
        self.db = db

        self.column = None
        self.movie_1 = None
        self.movie_2 = None

    def handle(self, parameter):
        """
        Handling the filtering request
        :param parameter:
        :return:
        """

        # Retreiving column name and titles of 1st and 2nd movie
        self.column = parameter[0]
        self.movie_1 = parameter[1]
        self.movie_2 = parameter[2]

        # Getting the results from the db
        try:
            results = self.db.execute_statement(self.sql_statement)
        except sqlite3.OperationalError as e:
            raise e

        # Return the results
        return results

    @property
    def sql_statement(self):
        """
        Statement to execute - sorting
        :return:
        """
        sql_statement = f"""SELECT {self.db.movies_table}.title, {self.db.movies_table}.{self.column}
                            FROM {self.db.movies_table}
                            WHERE {self.db.movies_table}.title IN ('{self.movie_1}', '{self.movie_2}')
                            ORDER BY {self.db.movies_table}.{self.column} desc
                            LIMIT 1"""
        return sql_statement

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
    Command Line Interface
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
                            type=str)

        # Filtering records
        parser.add_argument('--filter_by', help='filter records', action='store', nargs=2, type=str,
                            metavar=('column', 'value'))

        # Comparing records
        parser.add_argument('--compare', help='comparing records', action='store', nargs=3, type=str,
                            metavar=('column', 'movie1', 'movie2'))

        args = parser.parse_args()

        commands = dict()

        commands['update'] = args.update
        commands['sort_by'] = args.sort_by
        commands['filter_by'] = args.filter_by
        commands['compare'] = args.compare
        # print(args.filter_by[0])

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

        # Available handlers of commands
        handlers = [DataUpdater(db=db), DataSorter(db=db), DataFilter(db=db), DataCompare(db=db)]

        # Parse commands from args
        commands = CLInterface.get_args()

        # Performing commands
        Main.handle_commands(commands=commands, handlers=handlers)

    @staticmethod
    def format_results(results):
        """
        Return formatted result
        :param results:
        :return:
        """

        try:
            keys = results[0].keys()  # Loading keys
        except IndexError:
            # Empty results case
            raise
        except AttributeError:
            # Ready to print string
            return results

        formatted_results = '\n'  # Newline for styling
        for key in keys:
            formatted_results += ('{:30}'.format(str(key)) + " | ")

        formatted_results += '\n'

        for res in results:
            for key in res.keys():
                part = res[f'{key}']
                formatted_results += ('{:30}'.format(str(part)) + " | ")
            formatted_results += '\n'

        return formatted_results

    @staticmethod
    def handle_commands(commands, handlers):
        """
        Handling every command request by available handlers
        :param commands:
        :param handlers:
        :return:
        """

        for key in commands.keys():
            # Handle only not None commands
            if commands[f'{key}']:
                for handler in handlers:
                    if key == handler.get_keyword():
                        param = commands[key]
                        try:
                            results = handler.handle(parameter=param)
                            formatted_results = Main.format_results(results)
                            print(formatted_results)
                        except sqlite3.OperationalError as e:
                            print(str(e))
                        except IndexError:
                            print(handler.get_keyword() + ' error: No results found for ' + str(param))
                        except ValueError as e:
                            print(str(e))
                        break


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
