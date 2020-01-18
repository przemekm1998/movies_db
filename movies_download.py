import json

import requests
import _sqlite3


class MoviesDownload:
    """
    Downloading data using API and loading it to Database
    """

    def __init__(self, file_to_write, titles_to_get):
        self.files_to_write = file_to_write  # File name to write
        self.titles_to_get = titles_to_get
        self.titles = []
        self.results = []

    def read_titles(self):
        """
        Reading titles from a txt file
        """
        with open(self.titles_to_get, 'r') as read_file:
            for line in read_file:
                line = line.rstrip('\n')
                self.titles.append(line)

    def get_titles_using_api(self):
        """
        Downloading informations about movies based on read titles
        """
        API_KEY = '305043ae'  # API key to use API

        for title in self.titles:
            payload = {'t': title, 'r': 'json'}
            r = requests.get(f'http://www.omdbapi.com/?apikey={API_KEY}', params=payload).json()
            respond = json.dumps(r)  # Converting to proper json
            self.results.append(respond)

    def db_update(self):
        """
        Creating or updating existing db with movies
        """

