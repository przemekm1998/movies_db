import csv
import sqlite3

import requests


class DataDownload:
    """
    Reading the list of titles from txt file and downloading data using API
    """

    def __init__(self, titles_to_get):
        self.titles_to_get = titles_to_get
        self.titles = []

    def read_titles(self):
        """
        Reading titles from a txt file
        """

        try:
            #  Read from file
            with open(self.titles_to_get, 'r') as read_file:
                for line in read_file:
                    line = line.rstrip('\n')
                    self.titles.append(line)
        except FileNotFoundError as e:
            raise e

    def get_titles_using_api(self):
        """
        Downloading information about movies based on read titles
        """

        API_KEY = '305043ae'  # API key to use API
        results = list()  # List of json results

        for title in self.titles:
            payload = {'t': title, 'r': 'json'}
            r = requests.get(f'http://www.omdbapi.com/?apikey={API_KEY}', params=payload).json()
            respond = r  # Converting to proper json
            results.append(respond)

        return results


class DBConfig:
    """
    Inserting downloaded data to an SQL Database
    """

    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.conn.row_factory = sqlite3.Row  # Accessible object instead of plain tuple
        self.c = self.conn.cursor()

    def db_create(self):
        """
        Create tables if don't exist
        """

        try:
            # Try to create table
            with self.conn:
                self.c.execute("""
                    CREATE TABLE movies (
                    title TEXT,
                    year INTEGER,
                    rated TEXT,
                    released TEXT,
                    runtime TEXT,
                    genre TEXT,
                    director TEXT,
                    writer TEXT,
                    actors TEXT,
                    plot TEXT,
                    language TEXT,
                    country TEXT,
                    awards TEXT,
                    poster TEXT,
                    metascore INTEGER,
                    imdbRating REAL,
                    imdbVotes INTEGER,
                    imdbID INTEGER,
                    type TEXT,
                    DVD TEXT,
                    boxoffice REAL,
                    production TEXT,
                    website TEXT,
                    PRIMARY KEY (title, year)
                    );
                """)
        except sqlite3.OperationalError as e:
            print('Database with table movies already exists')
            raise e

    def insert_data(self, downloaded_results):
        """
        Inserting downloaded results to the database
        :param downloaded_results:
        :return:
        """

        for result in downloaded_results:
            try:
                with self.conn:
                    self.c.execute("""INSERT INTO movies VALUES 
                    (:title, :year, :rated, :released, :runtime, :genre, 
                    :director, :writer, :actors, :plot, :language, :country, 
                    :awards, :poster, :metascore, :imdbRating, :imdbVotes, 
                    :imdbID, :type, :DVD, :boxoffice, :production, :website);""",
                                   {'title': result['Title'],
                                    'year': result['Year'],
                                    'rated': result['Rated'],
                                    'released': result['Released'],
                                    'runtime': result['Runtime'],
                                    'genre': result['Genre'],
                                    'director': result['Director'],
                                    'writer': result['Writer'],
                                    'actors': result['Actors'],
                                    'plot': result['Plot'],
                                    'language': result['Language'],
                                    'country': result['Country'],
                                    'awards': result['Awards'],
                                    'poster': result['Poster'],
                                    'metascore': result['Metascore'],
                                    'imdbRating': result['imdbRating'],
                                    'imdbVotes': result['imdbVotes'],
                                    'imdbID': result['imdbID'],
                                    'type': result['Type'],
                                    'DVD': result['DVD'],
                                    'boxoffice': result['BoxOffice'],
                                    'production': result['Production'],
                                    'website': result['Website']
                                    })
            except sqlite3.IntegrityError as e:
                print(result['Title'] + ' already exists in the database')
                raise e

    def get_data_by_title(self, title):
        """
        Returning data based on inserted title
        :param title:
        :return:
        """
        self.c.execute("SELECT * FROM movies WHERE title = :title",
                       {'title': title})

        result = self.c.fetchall()

        return result

    def get_data_sort_by(self, parameter):
        """
        Return data sorted by parameter
        :param parameter:
        :return:
        """

        sql_query = f"SELECT * FROM movies WHERE {parameter} != 'N/A' ORDER BY {parameter} DESC"  # Query to run

        self.c.execute(sql_query)

        result = self.c.fetchall()
        print(result[0])

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
            fieldnames = data[0].keys()
            csv_writer = csv.DictWriter(csv_w, fieldnames=fieldnames, delimiter=',')

            csv_writer.writeheader()

            try:
                # Writing row in csv file for every result
                for result in data:
                    row = dict(result)
                    csv_writer.writerow(row)
            # List of results is empty
            except IndexError as e:
                print('Empty result')
                raise e
