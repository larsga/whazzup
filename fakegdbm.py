"""
A wrapper around gdbm using dbm so that we can get this to run locally
as well. The motivation for using gdbm over dbm is that on Linux
multiple processes can't open the same dbm. For some reason it works
on MacOS, so...
"""

import dbm

class FakeDBM:

    def __init__(self, file, flag):
        if 'c' in flag:
            flag = 'c'
        elif 'r' in flag:
            flag = 'r'
        self._dbm = dbm.open(file, flag)

    def has_key(self, key):
        return self._dbm.has_key(key)

    def __setitem__(self, key, value):
        self._dbm[key] = value

    def __getitem__(self, key):
        return self._dbm[key]

    def close(self):
        self._dbm.close()

open = FakeDBM
