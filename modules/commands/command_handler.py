from abc import ABCMeta, abstractmethod


class CommandHandler:
    """ Interface to ensure every handler of specific command uses
    the method handle and has a keyword """

    def __init__(self):
        self.results = []  # Keeping the results of all info

    __metaclass__ = ABCMeta

    @abstractmethod
    def handle(self, parameter):
        """
        The main handle function which all handlers must run
        :param parameter:
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def get_keyword(self):
        """
        Specific keyword of handler is required to identify it
        :return:
        """
        raise NotImplementedError
