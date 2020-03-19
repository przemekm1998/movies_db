import sqlite3

from modules.commands.command_handler import CommandHandler


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
