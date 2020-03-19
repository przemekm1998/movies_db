import sqlite3


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
        except sqlite3.OperationalError as e:
            # Not existing column
            raise e

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
