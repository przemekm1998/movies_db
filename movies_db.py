import argparse
import csv
import sqlite3
from datetime import datetime

import requests
from abc import ABCMeta, abstractmethod
from re import sub
from decimal import Decimal


class DBConfig:
    """ SQL Database management """

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
        return lambda column, value_to_change, new_value: f"""
                            UPDATE {table} SET {column} = {new_value} 
                            WHERE {column} = {value_to_change};"""


class CommandHandler:
    """ Interface to ensure every handler of specific command uses
    the method handle and has a keyword """

    def __init__(self):
        self.results = []  # Keeping the results of all info

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
    """ Handling updating database """

    def __init__(self, db):
        super().__init__()

        self.keyword = 'update'  # Constant keyword to recognize handler
        self.db = db  # DB to update

        self.api_key = '305043ae'  # static API key to use API

    @property
    def sql_empty_titles_statement(self):
        """
        Statement to execute - if director column is null than everything must be null
        :return:
        """
        sql_statement = f"""SELECT {self.db.movies_table}.title 
                            FROM {self.db.movies_table} 
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

        results = []

        # If user wanted the update
        if parameter:
            # Getting titles with empty data
            empty_titles = self.db.execute_statement(
                sql_statement=self.sql_empty_titles_statement)

            # Convrting SQL Row Objects to str
            empty_titles = [title['Title'] for title in empty_titles]

            # Downloading data for empty titles using API
            api_data = self.download_data(empty_titles)

            # Updating database with downloaded data
            results = self.update_data(api_data)

            # Changing box office N/A value to null for further conveniance
            self.na_to_null('box_office')

        return results

    def download_data(self, titles):
        """
        Downloading data using API
        :param titles:
        :return:
        """

        for title in titles:
            payload = {'t': title,
                       'r': 'json'}  # Get full data based on title in json format
            respond = requests.get(f'http://www.omdbapi.com/?apikey={self.api_key}',
                                   params=payload).json()
            self.results.append(respond)

        return self.results

    def update_data(self, downloaded_results):
        """
        Updating the data in the database
        :param downloaded_results:
        :return:
        """

        for result in downloaded_results:

            # Insert data
            try:
                self.insert_data(result)
                info = {'Title': result['Title'], 'Status': 'Updated'}
            except KeyError:
                info = {'Title': result['Title'], 'Status': 'Not Found'}

            self.results.append(info)

        return self.results

    def insert_data(self, result):
        """
        Inserting data to the db
        :return:
        """

        try:
            # Converting money
            money_str = result['BoxOffice']
            money_to_insert = self.convert_integers(money_str)

            # Converting votes
            votes_str = result['imdbVotes']
            votes_to_insert = self.convert_integers(votes_str)

            with self.db.conn:
                self.db.c.execute(self.sql_data_update_statement,
                                  {'title': result['Title'], 'year': result['Year'],
                                   'runtime': result['Runtime'],
                                   'genre': result['Genre'],
                                   'director': result['Director'],
                                   'writer': result['Writer'],
                                   'cast': result['Actors'],
                                   'language': result['Language'],
                                   'country': result['Country'],
                                   'awards': result['Awards'],
                                   'imdbRating': result['imdbRating'],
                                   'imdbVotes': votes_to_insert,
                                   'boxoffice': money_to_insert
                                   })
        except KeyError:
            raise

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
                print("Column doesn't exist")

    @staticmethod
    def convert_integers(int_to_convert):
        """
        Converting money to integer
        :return:
        """

        try:
            money_int = Decimal(sub(r'[^\d.]', '', int_to_convert))
        except Exception:
            # Tried to handle particular exception but can't catch it
            return str(int_to_convert)

        return str(money_int)

    def get_keyword(self):
        return self.keyword


class DataSorter(CommandHandler):
    """ Handling the filter command request """

    def __init__(self, db):
        super().__init__()

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
        sql_statement = f"""SELECT {self.db.movies_table}.title, 
                            {self.db.movies_table}.{self.parameter}
                            FROM {self.db.movies_table}
                            ORDER BY {self.parameter} DESC"""
        return sql_statement

    def handle(self, parameter):
        """
        Handling the sorting command request
        :return:
        """

        self.parameter = parameter

        # Get the results from db
        try:
            self.results = self.db.execute_statement(self.sql_statement)
        except sqlite3.OperationalError as e:
            raise e

        return self.results

    def get_keyword(self):
        return self.keyword


class DataFilter(CommandHandler):
    """ Handling the filtering command request """

    def __init__(self, db):
        super().__init__()

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
            self.results = self.db.execute_statement(self.sql_statement)
        except sqlite3.OperationalError as e:
            raise e

        # Return the results
        return self.results

    @property
    def sql_statement(self):
        """
        Statement to execute - filtering
        :return:
        """
        sql_statement = f"""SELECT {self.db.movies_table}.title, 
                            {self.db.movies_table}.{self.column}
                            FROM {self.db.movies_table}
                            WHERE {self.db.movies_table}.{self.column} 
                            LIKE '%{self.value}%';"""
        return sql_statement

    def get_keyword(self):
        return self.keyword


class DataCompare(CommandHandler):
    """ Comparing two titles """

    def __init__(self, db):
        super().__init__()

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
            self.results = self.db.execute_statement(self.sql_statement)
        except sqlite3.OperationalError as e:
            raise e

        # Return the results
        return self.results

    @property
    def sql_statement(self):
        """
        Statement to execute - comparing
        :return:
        """
        sql_statement = f"""SELECT {self.db.movies_table}.title, 
                            {self.db.movies_table}.{self.column}
                            FROM {self.db.movies_table}
                            WHERE {self.db.movies_table}.title IN ('{self.movie_1}', 
                            '{self.movie_2}')
                            ORDER BY {self.db.movies_table}.{self.column} desc
                            LIMIT 1"""
        return sql_statement

    def get_keyword(self):
        return self.keyword


class DataInsert(CommandHandler):
    """ Inserting new title """

    def __init__(self, db):
        super().__init__()

        self.keyword = 'insert'
        self.db = db
        self.title_to_write = None

    @property
    def sql_statement(self):
        """
        Statement to execute - inserting
        :return:
        """
        sql_statement = f"""INSERT INTO {self.db.movies_table}(title)
                            VALUES('{self.title_to_write}')"""
        return sql_statement

    def handle(self, parameter):
        """
        Handle the insert title request
        :param parameter:
        :return:
        """

        # Inserting every or single title given by user
        if type(parameter) is list:
            for title in parameter:
                self.insert_title(title)

                info = {'Title': title, 'Status': 'Inserted'}
                self.results.append(info)
        else:
            self.insert_title(parameter)

            info = {'Title': parameter, 'Status': 'Inserted'}
            self.results.append(info)

        return self.results

    def insert_title(self, title):
        """
        Inserting the title to the db
        :param title:
        :return:
        """
        self.title_to_write = title

        try:
            self.db.c.execute(self.sql_statement)
        except sqlite3.OperationalError as e:
            raise e
        except sqlite3.IntegrityError as e:
            raise e

    def get_keyword(self):
        return self.keyword


class DataDelete(CommandHandler):
    """ Deleting title or titles """

    def __init__(self, db):
        self.keyword = 'delete'
        self.db = db
        self.title_to_delete = None

    @property
    def sql_statement(self):
        """
        Statement to execute - inserting
        :return:
        """
        sql_statement = f"""DELETE FROM {self.db.movies_table}
                            WHERE title = '{self.title_to_delete}'"""
        return sql_statement

    def handle(self, parameter):
        """
        Handle the delete title request
        :param parameter:
        :return:
        """

        info = dict()  # Keeping record for particular row
        results = []  # Keeping the results of all info

        # Inserting every or single title given by user
        if type(parameter) is list:
            for title in parameter:
                self.delete_title(title)

                info['Title'] = title
                info['Status'] = 'Deleted'
                results.append(info)
        else:
            self.delete_title(parameter)

            info['Title'] = parameter
            info['Status'] = 'Deleted'
            results.append(info)

        # Return the information about inserted data
        return 'All titles successfully deleted!'

    def delete_title(self, title):
        """
        Deleting the title from the db
        :param title:
        :return:
        """
        self.title_to_delete = title

        try:
            self.db.c.execute(self.sql_statement)
        except sqlite3.OperationalError as e:
            raise e
        except sqlite3.IntegrityError as e:
            raise e

        print(str(title) + " deleted from the database!")

    def get_keyword(self):
        return self.keyword


class DataHighscores(CommandHandler):
    """ Showing highscores """

    def __init__(self, db):
        self.keyword = 'highscores'
        self.db = db

        self.columns = ['RUNTIME', 'IMDb_Rating', 'BOX_OFFICE', 'IMDb_votes']
        self.col_name = None

    @property
    def sql_statement(self):
        """
        Statement to execute - get highscore with column name
        :return:
        """
        sql_statement = f"""SELECT name as col_name, 
                            MAX({self.db.movies_table}.{self.col_name}) as max_val, 
                            {self.db.movies_table}.title
                            FROM PRAGMA_TABLE_INFO('{self.db.movies_table}')
                            JOIN {self.db.movies_table}
                            ON name = '{self.col_name}';"""
        return sql_statement

    def handle(self, parameter):
        """
        Handle the highscores request
        :param parameter:
        :return:
        """

        results = []

        if parameter:

            for column in self.columns:
                self.col_name = column
                result = self.db.execute_statement(self.sql_statement)
                results.append(result[0])

        return results

    def get_keyword(self):
        return self.keyword


class CSVWriter:
    """ Creating csv file """

    def __init__(self):
        self.title = 'None'

    def write_csv(self, keyword, data):
        """
        Saving results query as csv file
        :param keyword:
        :param data:
        :return:
        """

        try:
            fieldnames = data[0].keys()  # Getting the csv fieldnames
        except IndexError as e:
            raise e

        self.title = self.create_title(keyword)

        with open(self.title, 'w') as csv_w:

            csv_writer = csv.DictWriter(csv_w, fieldnames=fieldnames, delimiter=',')
            csv_writer.writeheader()

            # Writing row in csv file for every result
            for result in data:
                row = dict(result)
                csv_writer.writerow(row)

    @staticmethod
    def create_title(keyword):
        """
        Creating title for every operation with timestamp
        :param keyword:
        :return:
        """

        date = str(datetime.now().strftime('%Y-%m-%d_%H:%M:%S'))
        title = date

        title += '_' + keyword + '.csv'

        return title


class CLInterface:
    """ Command Line Interface """

    @staticmethod
    def get_args():
        """
        Loading arguments given by user
        :return:
        """

        # Creating parser
        parser = argparse.ArgumentParser(prog='my_program',
                                         description='Working with movies DB.')
        parser.add_argument('--version', action='version', version='1.0.0')

        # Updating records
        parser.add_argument('--update', help='update records', action='store_true')

        # Sorting records
        parser.add_argument('--sort_by', help='sort records', action='store', nargs=1,
                            choices=['id', 'title', 'year', 'runtime', 'genre',
                                     'director', 'cast', 'writer', 'language',
                                     'country',
                                     'awards', 'imdb_rating', 'imdb_votes',
                                     'box_office'],
                            type=str)

        # Filtering records
        parser.add_argument('--filter_by', help='filter records', action='store',
                            nargs=2, type=str,
                            metavar=('column', 'value'))

        # Comparing records
        parser.add_argument('--compare', help='comparing records', action='store',
                            nargs=3, type=str,
                            metavar=('column', 'movie1', 'movie2'))

        # Inserting titles
        parser.add_argument('--insert', help='inserting records', action='store',
                            nargs='+', type=str,
                            metavar='title')

        # Deleting titles
        parser.add_argument('--delete', help='deleting records', action='store',
                            nargs='+', type=str,
                            metavar='title')

        # Highscores
        parser.add_argument('--highscores', help='highscores by every column',
                            action='store_true')

        # Writing csv
        parser.add_argument('--write_csv', help='saving results as csv file',
                            action='store_true')

        # Writing csv
        parser.add_argument('--db_name', help='select the db', action='store',
                            default='movies.sqlite')

        args = parser.parse_args()

        commands = dict()

        commands['update'] = args.update
        commands['sort_by'] = args.sort_by
        commands['filter_by'] = args.filter_by
        commands['compare'] = args.compare
        commands['insert'] = args.insert
        commands['delete'] = args.delete
        commands['highscores'] = args.highscores
        commands['write_csv'] = args.write_csv
        commands['db_name'] = args.db_name

        return commands


class Main:
    """ Main program class """

    @staticmethod
    def main():
        """
        Main program function
        :return:
        """

        # Parse commands from args
        commands = CLInterface.get_args()

        # Initialization of DB connection
        db_name = commands['db_name']
        db = DBConfig(db_name=db_name)

        # Available handlers of commands
        handlers = [DataUpdater(db=db), DataSorter(db=db), DataFilter(db=db),
                    DataCompare(db=db), DataInsert(db=db),
                    DataDelete(db=db), DataHighscores(db=db)]

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
            keys = results[0].keys()
        except IndexError as e:
            # Empty results case
            raise e
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

        list_of_commands = commands.keys()
        write_csv = list_of_commands.pop('write_csv')

        for key in list_of_commands:

            # Handle only not None commands
            if commands[f'{key}']:
                for handler in handlers:

                    if key == handler.get_keyword():
                        param = commands[key]

                        try:
                            results = handler.handle(parameter=param)

                            if write_csv:
                                # User wants to print to csv
                                csv_writer = CSVWriter()
                                csv_writer.write_csv(keyword=key, data=results)
                            else:
                                # Standard print to console
                                formatted_results = Main.format_results(results)
                                print(formatted_results)

                        except sqlite3.OperationalError as e:
                            print(str(e))

                        except IndexError:
                            print(handler.get_keyword() +
                                  ' error: No results found for ' + str(param))

                        except ValueError as e:
                            print(str(e))
                        break


if __name__ == '__main__':
    Main.main()
