#!/usr/bin/env python3

import os
import sys
import signal
import dropbox
import json
from queuelib import FifoDiskQueue
from collections import namedtuple
from datetime import datetime, date
import json
from pprint import pprint

__version__ = 0.1

def main():

    with open('sbg-diff.json') as data_file:
        data = json.load(data_file)
        queue = FifoDiskQueue('sbg_2017_08_16_diff')
        for d in data:

            file = FakeFile()
            file.path_display = d
            queue.push(str.encode(json.dumps(file.__dict__)))

        queue.close()


class FakeFileList(object):
       has_more = False
       entries = list()

class FakeFile(object):
      path_display = None
      size = 0
      server_modified = None
      client_modified = None

if __name__ == '__main__':
  main()
