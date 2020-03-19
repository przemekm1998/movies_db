from modules.commands.command_handler import CommandHandler


class DataHighscores(CommandHandler):
    """ Showing highscores """

    def __init__(self, db):
        super().__init__()
        self.keyword = 'highscores'
        self.db = db

        self.columns = ('RUNTIME', 'IMDb_Rating', 'BOX_OFFICE', 'IMDb_votes')
        self.col_name = None

    @property
    def sql_statement(self):
        """
        Statement to execute - get highscore with column name
        :return:
        """
        sql_statement = f"""SELECT name as col_name, 
                            MAX({self.db.movies_table}.{self.col_name}) as max_val, 
                            {self.db.movies_table}.title
                            FROM PRAGMA_TABLE_INFO('{self.db.movies_table}')
                            JOIN {self.db.movies_table}
                            ON name = '{self.col_name}';"""
        return sql_statement

    def handle(self, parameter):
        """
        Handle the highscores request
        :param parameter:
        :return:
        """

        if parameter:

            for column in self.columns:
                self.col_name = column
                result = self.db.execute_statement(self.sql_statement)
                self.results.append(result[0])

        return self.results

    def get_keyword(self):
        return self.keyword
