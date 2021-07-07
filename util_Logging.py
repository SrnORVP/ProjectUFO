import logging


class LogWarp:
    @staticmethod
    def wt(string):
        string = string if isinstance(string, str) else str(string)
        logging.info(string, stacklevel=3)

    @staticmethod
    def bk(string):
        logging.info(string.center(100, '-'))

    @staticmethod
    def nl():
        logging.info('\n')

    def __init__(self):
        custom = '%(filename)-20s | %(funcName)-25s | %(lineno)-3d | %(message)s'
        logging.basicConfig(filename='runtime.log', filemode='w', level=20, format=custom)
        self._w = self.wt
