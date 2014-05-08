#!/usr/bin/env python2

from pyftpdlib import ftpserver
import threading, re, io, random

class NeguraFsOpenedFile:
    neguraClient = None

    def __init__(self, op, name):
        self.op = op
        self.name = name
        self.size = op["size"]
        self.closed = False
        self.position = 0

    def close(self):
        self.closed = True

    def readable(self):
        return True

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            self.position = offset
        elif whence == io.SEEK_CUR:
            self.position += offset
        elif whence == io.SEEK_END:
            self.position = self.size + offset

    def seekable(self):
        return True

    def writable(self):
        return False

    def read(self, n):
        ret = NeguraFsOpenedFile.neguraClient.readFromFile(self.op["opid"], \
                self.position, n)
        self.position += len(ret)
        return ret

class NeguraFsFile:
    def __init__(self, op):
        self.op = op
        self.size = self.op["size"]
        self.date = self.op["date"]
        path = op["path"].split("/")
        if len(path[-1]) > 0:
            self.name = path[-1]
        else:
            self.name = path[-2]
    def open(self):
        return NeguraFsOpenedFile(self.op, self.name)

class FakeStat:
    def __init__(self, isFile, size, date=1):
        if isFile:
            self.st_mode = 33204
        else:
            self.st_mode = 16893
        self.st_nlink = 1
        self.st_size = size
        self.st_uid = 1
        self.st_gid = 1
        self.st_mtime = date
        self.st_dev = 1
        self.st_ino = random.randint(1, 2000000000)

class NeguraFs(ftpserver.AbstractedFS):
    ops = []
    opsLock = threading.RLock()
    struct = {}

    @staticmethod
    def addOperation(op):
        with NeguraFs.opsLock:
            NeguraFs.ops.append(op)

            loc = NeguraFs.struct

            path = re.sub("/+", "/", op["path"])
            if path[-1] == "/":
                return path[:-1]
            path = path[1:]

            names = path.split("/")
            for name in names[:-1]:
                if not loc.has_key(name):
                    loc[name] = {}
                loc = loc[name]
            loc[names[-1]] = NeguraFsFile(op)

    def __init__(self, root, cmd_channel):
        ftpserver.AbstractedFS.__init__(self, root, cmd_channel)
        self._cwd = root
        self._cwdLock = threading.RLock()

    def _normalize(self, path):
        # return path without multiple "/" and ending "/"
        path = re.sub("/+", "/", path)
        if path[-1] == "/":
            return path[:-1]
        return path

    def _absPath(self, path):
        if path[0] == "/":
            return self._normalize(path)
        else:
            with self._cwdLock:
                return self._normalize(self._cwd + "/" + path)

    def _nameSequence(self, path):
        return self._absPath(path).split("/")[1:]

    def _getItem(self, path):
        loc = NeguraFs.struct
        for name in self._nameSequence(path):
            if not loc.has_key(name):
                return None
            loc = loc[name]
        return loc

    def validpath(self, path):
        item = self._getItem(path)
        return item != None

    def open(self, filename, mode):
        return self._getItem(filename).open()

    def chdir(self, path):
        with self._cwdLock:
            self._cwd = path

    def mkdir(self, path):
        raise NotImplementedError

    def listdir(self, path):
        lis = self._getItem(path).keys()
        lis = [u.encode("utf-8") for u in lis]
        return lis

    def rmdir(self, path):
        raise NotImplementedError

    def remove(self, path):
        raise NotImplementedError

    def rename(self, src, dst):
        raise NotImplementedError
        
    def stat(self, path):
        item = self._getItem(path)
        if isinstance(item, dict):
            return FakeStat(False, 4096)
        else:
            return FakeStat(True, item.size, item.date)

    lstat = stat

    def isfile(self, path):
        return isinstance(self._getItem(path), NeguraFsFile)

    def islink(self, path):
        return False

    def isdir(self, path):
        return isinstance(self._getItem(path), dict)

    def getsize(self, path):
        return self._getItem(path).size

    def getmtime(self, path):
        return self._getItem(path).date

    def realpath(self, path):
        return path

    def lexists(self, path):
        return self._getItem(path) != None

    def get_user_by_uid(self, uid):
        return "owner"

    def get_group_by_gid(self, gid):
        return "group"

class NeguraFtpServer:
    def __init__(self, port, neguraClient):
        authorizer = ftpserver.DummyAuthorizer()
        authorizer.add_anonymous("/")
        ftp_handler = ftpserver.FTPHandler
        ftp_handler.authorizer = authorizer
        ftp_handler.abstracted_fs = NeguraFs
        address = ('', port)
        self.ftpd = ftpserver.FTPServer(address, ftp_handler)
        NeguraFsOpenedFile.neguraClient = neguraClient

    def start(self):
        self.ftpd.serve_forever()
