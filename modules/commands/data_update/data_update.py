import concurrent.futures
import sqlite3
from decimal import Decimal
from re import sub

import requests

from modules.commands.command_handler import CommandHandler
from modules.db_config.db_config import DBConfig


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
            with concurrent.futures.ThreadPoolExecutor() as executor:
                api_data = executor.map(self.download_data, empty_titles)

            # Updating database with downloaded data
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = [executor.map(self.update_data, api_data)]

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
        except KeyError as e:
            raise e

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
