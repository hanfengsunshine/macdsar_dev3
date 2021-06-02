from logging import FileHandler
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from threading import Thread
from queue import Queue
import os


class IntegralRotatingFileHandler(TimedRotatingFileHandler):
    def computeRollover(self, currentTime):
        if self.when[0] == 'W' or self.when == 'MIDNIGHT':
            return super().computeRollover(currentTime)
        return ((currentTime // self.interval) + 1) * self.interval

    def _open(self):
        """
        Open the current base file with the (original) mode and encoding.
        Return the resulting stream.
        """
        stream = open(self.baseFilename, self.mode, encoding=self.encoding)
        # try:
        #     os.symlink(self.baseFilename, "{}.link".format(self.baseFilename))
        # except OSError as e:
        #     pass
        return stream

class AsyncHandlerMixin(object):
    def __init__(self, *args, **kwargs):
        super(AsyncHandlerMixin, self).__init__(*args, **kwargs)
        self.__queue = Queue()
        self.__thread = Thread(target=self.__loop)
        self.__thread.daemon = True
        self.__thread.start()

    def emit(self, record):
        self.__queue.put(record)

    def __loop(self):
        while True:
            record = self.__queue.get()
            try:
                super(AsyncHandlerMixin, self).emit(record)
            except:
                pass


class AsyncFileHandler(AsyncHandlerMixin, FileHandler):
    pass


class AsyncRotatingFileHandler(AsyncHandlerMixin, RotatingFileHandler):
    pass


class AsyncTimedRotatingFileHandler(AsyncHandlerMixin, IntegralRotatingFileHandler):
    pass