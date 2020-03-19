import sqlite3

from modules.commands.command_handler import CommandHandler


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
