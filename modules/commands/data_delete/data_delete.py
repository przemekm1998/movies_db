import sqlite3

from modules.commands.command_handler import CommandHandler


class DataDelete(CommandHandler):
    """ Deleting titles """

    def __init__(self, db):
        super().__init__()

        self.keyword = 'delete'
        self.db = db
        self.title_to_delete = None

    @property
    def sql_statement(self):
        """
        Statement to execute - deleting
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

        # Inserting every title given by user
        if type(parameter) is list:
            for title in parameter:
                self.delete_title(title)

                info = {'Title': title, 'Status': 'Deleted'}
                self.results.append(info)
        else:
            self.delete_title(parameter)

            info = {'Title': parameter, 'Status': 'Deleted'}
            self.results.append(info)

        return self.results

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

    def get_keyword(self):
        return self.keyword
