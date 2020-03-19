import argparse


class CLInterface(object):
    """ Command Line Interface """

    @staticmethod
    def get_args():
        """
        Loading arguments given by user
        :return:
        """

        # Creating parser
        parser = argparse.ArgumentParser(prog='my_program',
                                         description='Working with movies DB.')
        parser.add_argument('--version', action='version', version='1.0.0')

        # Updating records
        parser.add_argument('--update', help='update records', action='store_true')

        # Sorting records
        parser.add_argument('--sort_by', help='sort records', action='store', nargs=1,
                            choices=['id', 'title', 'year', 'runtime', 'genre',
                                     'director', 'cast', 'writer', 'language',
                                     'country',
                                     'awards', 'imdb_rating', 'imdb_votes',
                                     'box_office'],
                            type=str)

        # Filtering records
        parser.add_argument('--filter_by', help='filter records', action='store',
                            nargs=2, type=str,
                            metavar=('column', 'value'))

        # Comparing records
        parser.add_argument('--compare', help='comparing records', action='store',
                            nargs=3, type=str,
                            metavar=('column', 'movie1', 'movie2'))

        # Inserting titles
        parser.add_argument('--insert', help='inserting records', action='store',
                            nargs='+', type=str,
                            metavar='title')

        # Deleting titles
        parser.add_argument('--delete', help='deleting records', action='store',
                            nargs='+', type=str,
                            metavar='title')

        # Highscores
        parser.add_argument('--highscores', help='highscores by every column',
                            action='store_true')

        # Writing csv
        parser.add_argument('--write_csv', help='saving results as csv file',
                            action='store_true', const=False)

        # Writing csv
        parser.add_argument('--db_name', help='select the db', action='store',
                            default='movies.sqlite')

        args = parser.parse_args()

        commands = dict()

        commands['update'] = args.update
        commands['sort_by'] = args.sort_by
        commands['filter_by'] = args.filter_by
        commands['compare'] = args.compare
        commands['insert'] = args.insert
        commands['delete'] = args.delete
        commands['highscores'] = args.highscores
        commands['write_csv'] = args.write_csv
        commands['db_name'] = args.db_name

        return commands
