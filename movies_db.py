import argparse
import csv
import sqlite3
import requests


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

        sql_statement = f"SELECT * FROM {self.movies_table} WHERE director IS NULL"
        self.c.execute(sql_statement)
        titles = self.c.fetchall()

        return [title['Title'] for title in titles]

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
                     'boxoffice': result['BoxOffice']
                     })

    def filter_data(self, parameter, value):
        """
        Returning data based on inserted title
        :return:
        """
        self.c.execute(f"SELECT * FROM {self.movies_table} WHERE {parameter} LIKE '%{value}%';")

        result = self.c.fetchall()

        return result

    def sort_data(self, parameter):
        """
        Return data sorted by parameter
        :param parameter:
        :return:
        """

        # Query to run
        sql_query = f"SELECT * FROM {self.movies_table} WHERE {parameter} != 'N/A' ORDER BY {parameter} DESC"

        self.c.execute(sql_query)
        result = self.c.fetchall()

        return result


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

# class CLInterface:
#     """
#     Command Line Interface Class
#     """
#
#     # Creating parser
#     parser = argparse.ArgumentParser(description='Processing the commands.')
#
#     # Loading titles from file and downloading data based on them
#     parser.add_argument('--read', metavar='read', type=str, help='file to read titles from')
#
#     # Reading data from database and writing it to csv
#     parser.add_argument('--', metavar='print', type=str, help='print input',
#                         default='eluwina', choices=['gitara', 'siema'])
#
#     args = parser.parse_args()
#     print(args.accumulate(args.integers))
