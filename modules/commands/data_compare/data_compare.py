import sqlite3

from modules.commands.command_handler import CommandHandler


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
