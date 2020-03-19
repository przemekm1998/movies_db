import csv
from datetime import datetime


class CSVWriter:
    """ Creating csv file """

    def __init__(self):
        self.title = 'None'

    def write_csv(self, keyword, data):
        """
        Saving results query as csv file
        :param keyword:
        :param data:
        :return:
        """

        try:
            fieldnames = data[0].keys()  # Getting the csv fieldnames
        except IndexError as e:
            raise e

        self.title = self.create_title(keyword)

        with open(self.title, 'w') as csv_w:

            csv_writer = csv.DictWriter(csv_w, fieldnames=fieldnames, delimiter=',')
            csv_writer.writeheader()

            # Writing row in csv file for every result
            for result in data:
                row = dict(result)
                csv_writer.writerow(row)

    @staticmethod
    def create_title(keyword):
        """
        Creating title for every operation with timestamp
        :param keyword:
        :return:
        """

        date = str(datetime.now().strftime('%Y-%m-%d_%H:%M:%S'))
        title = date

        title += '_' + keyword + '.csv'

        return title
