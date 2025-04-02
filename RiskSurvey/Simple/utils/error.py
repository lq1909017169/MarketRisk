from Simple.utils.connect import Connect


class ColumnFindError(Exception):

    def __init__(self, message):
        super().__init__(message)


class Log:

    def __init__(self, connect: Connect, msg: str):
        self.connect = connect
        self.msg = msg


