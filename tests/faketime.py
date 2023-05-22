import time
from datetime import datetime


class FakeTime:

    def __init__(self):
        self.time_offset = 0

    def sleep(self, time):
        self.time_offset += time

    def time(self):
        return time.time() + self.time_offset

    def gmtime(self):
        return datetime.fromtimestamp(self.time()).timetuple()
