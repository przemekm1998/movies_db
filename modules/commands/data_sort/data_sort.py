import sqlite3

from modules.commands.command_handler import CommandHandler


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
