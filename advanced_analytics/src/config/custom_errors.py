import errno


class FileNotFoundError(IOError):
    """
    No such file or directory
    """

    err_no = errno.ENOENT
    message = "No such file or directory:"

    def __init__(self, dir):
        msg = "{} {}".format(self.message, dir)
        super(FileNotFoundError, self).__init__(self.err_no, msg)

    # def __str__(self):
    #     return "[Errno {}] {}".format(self.err_no, self.message)
