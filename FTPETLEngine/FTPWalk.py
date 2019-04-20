"""
Connects to an FTP server and scans the contents
"""
import os
import re


class FTPWalk:
    """
    Contains methods for traversing an FTP server using a BFS algorithm
    Source: https://stackoverflow.com/a/43799926
    """
    def __init__(self, connection):
        """
        Stores the FTP server connection

        :param connection:  An FTP server connection stream
        :type conection:    ftplib.FTP
        """
        self.connection = connection

    def walk(self, path='/'):
        """
        Walks through an FTP server's directory tree using a BFS algorithm.

        :param path:    Directory in which to create a tree
        """
        dirs, nondirs = self.listdir(path)
        yield path, dirs, nondirs
        for name in dirs:
            path = os.path.join(path, name)
            yield from self.walk(path)
            self.connection.cwd('..')
            path = os.path.dirname(path)

    def listdir(self, _path):
        """
        Generates a file-directory tree given a path

        :param _path:   Inherited from walk

        :returns:       Lists of directory and file names within _path
        """
        file_list, dirs, nondirs = [], [], []
        self.connection.cwd(_path)
        self.connection.retrlines(
            'LIST', lambda x: file_list.append(x.split()))
        for info in file_list:
            ls_type, name = info[-2], info[-1]
            if 'DIR' in ls_type:
                dirs.append(name)
            else:
                nondirs.append(name)
        return dirs, nondirs

    def get_files(self, dir_ptrn='', file_ptrn=''):
        """
        Generates a sorted list of files on the FTP server; directory and file
        names match the specified patterns

        :param dir_ptrn:    Regular expression identifying directories that
                            contain relevant files
        :type dir_ptrn:     str
        :param file_ptrn:   Regular expression identifying relevant files
        :type file_ptrn:    str

        :returns:           A list of strings, each of which is the full path
                            to a specific file
        """
        files = []
        for item in self.walk():
            if re.match(dir_ptrn, item[0]) and item[2]:
                for file in item[2]:
                    if re.match(file_ptrn, file):
                        files.append(os.path.join(item[0], file))
        files.sort()
        return files
