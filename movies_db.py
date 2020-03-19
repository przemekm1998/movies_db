import sqlite3

from modules.cli.cli import CLInterface
from modules.commands.data_compare.data_compare import DataCompare
from modules.commands.data_delete.data_delete import DataDelete
from modules.commands.data_filter.data_filter import DataFilter
from modules.commands.data_highscores.data_highscores import DataHighscores
from modules.commands.data_insert.data_insert import DataInsert
from modules.commands.data_sort.data_sort import DataSorter
from modules.commands.data_update.data_update import DataUpdater
from modules.csv_writer.csv_writer import CSVWriter
from modules.db_config.db_config import DBConfig


class Main:
    """ Main program class """

    @staticmethod
    def main():
        """
        Main program function
        :return:
        """

        # Parse commands from args
        commands = CLInterface.get_args()

        # Initialization of DB connection
        db_name = commands['db_name']
        db = DBConfig(db_name=db_name)

        # Available handlers of commands
        handlers = [DataUpdater(db=db), DataSorter(db=db), DataFilter(db=db),
                    DataCompare(db=db), DataInsert(db=db),
                    DataDelete(db=db), DataHighscores(db=db)]

        # Performing commands
        Main.handle_commands(commands=commands, handlers=handlers)

    @staticmethod
    def format_results(results):
        """
        Return formatted result
        :param results:
        :return:
        """

        try:
            keys = results[0].keys()
        except IndexError as e:
            # Empty results case
            raise e
        except AttributeError:
            # Ready to print string
            return results

        formatted_results = '\n'  # Newline for styling

        for key in keys:
            formatted_results += ('{:30}'.format(str(key)) + " | ")

        formatted_results += '\n'

        for res in results:
            for key in res.keys():
                part = res[f'{key}']
                formatted_results += ('{:30}'.format(str(part)) + " | ")
            formatted_results += '\n'

        return formatted_results

    @staticmethod
    def return_output(write_csv, key, results):
        if write_csv:
            # User wants to print to csv
            csv_writer = CSVWriter()
            csv_writer.write_csv(keyword=key, data=results)
        else:
            # Standard print to console
            formatted_results = Main.format_results(results)
            print(formatted_results)

    @staticmethod
    def handle_commands(commands, handlers):
        """
        Handling every command request by available handlers
        :param commands:
        :param handlers:
        :return:
        """

        list_of_commands = tuple([command for command in commands.keys()
                                  if command != 'write_csv'])
        write_csv = commands['write_csv']

        for key in list_of_commands:

            # Handle only not None commands
            if commands[f'{key}']:
                for handler in handlers:
                    if key == handler.get_keyword():
                        param = commands[key]
                        try:
                            results = handler.handle(parameter=param)
                        except sqlite3.OperationalError as e:
                            print(str(e))
                            break

                        try:
                            Main.return_output(write_csv=write_csv, key=key,
                                               results=results)
                        except IndexError:
                            print(handler.get_keyword() +
                                  ' error: No results found for ' + str(param))
                        except ValueError as e:
                            print(str(e))
                        break


if __name__ == '__main__':
    Main.main()
