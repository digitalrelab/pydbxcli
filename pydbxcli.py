#!/usr/bin/env python3

import os
import sys
import argparse
import signal
import dropbox
import json
from queuelib import FifoDiskQueue
from collections import namedtuple
from datetime import datetime, date

__version__ = 0.1


def signal_handler(signal, frame):
    print('Exiting...')
    sys.exit(0)


def main():
    # signal handler for clean exit on CTRL+C
    signal.signal(signal.SIGINT, signal_handler)

    # create the top-level parser
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparser = parser.add_subparsers(title='Available Commands')
    parser.add_argument('--dropbox_token',
                        type=str,
                        default=None,
                        help='This will overrides the environment variable DROPBOX_TOKEN')

    # create the parser for the "version" command
    parser_version = subparser.add_parser(name='version',
                                          help='prints version information')
    parser_version.set_defaults(func=version)

    # create the parser for the "status" command
    parser_status = subparser.add_parser(name='status',
                                         help='prints account status information')
    parser_status.set_defaults(func=status)

    # create the parser for the "ls" command
    parser_ls = subparser.add_parser(name='ls',
                                     help='list files and directories')
    parser_ls.add_argument('-r',
                           action='store_true',
                           default=False,
                           help='Recursively list files and directories')
    parser_ls.add_argument('path',
                           type=str,
                           default='/',
                           help='which path from which to list')
    parser_ls.add_argument('--excludePaths',
                           nargs='*',
                           default=[],
                           help=("a list of folder paths to ignore. "
                                 "example: --excludePaths '/Team Folders/IgnoreThisFolder' "
                                 "'/Team Folders/IgnoreThisFolderToo'")
                           )
    parser_ls.add_argument('--pushFilesToQueue',
                         type=str,
                         default=None,
                         help='push files to the given queue name')
    parser_ls.set_defaults(func=ls)

    # create the parser for the "get" command
    parser_ls = subparser.add_parser(name='get',
                                     help='download files from Dropbox to local folder')
    parser_ls.add_argument('-r',
                           action='store_true',
                           default=False,
                           help='Recursively download files and directories')
    parser_ls.add_argument('src_path',
                           type=str,
                           default='/',
                           help='which path from Dropbox from which to download')
    parser_ls.add_argument('dest_path',
                           type=str,
                           default='.',
                           help='which path on the local machine to store downloads')
    parser_ls.add_argument('--pullFilesFromQueue',
                          type=str,
                          default=None,
                          help='pull files from the given queue name')
    parser_ls.add_argument('--excludePaths',
                           nargs='*',
                           default=[],
                           help=("a list of folder paths to ignore. "
                                 "example: --excludePaths '/Team Folders/IgnoreThisFolder' "
                                 "'/Team Folders/IgnoreThisFolderToo'")
                                 )
    parser_ls.set_defaults(func=get)

    #add parser for peeking at queues
    parser_ls = subparser.add_parser(name='peek',
                                  help='peek at a queue')
    parser_ls.add_argument('--queueName',
                        help='Name of queue to peek')
    parser_ls.add_argument('--flush',
                        action='store_true',
                        default=False,
                        help='flush queue after peek')
    parser_ls.set_defaults(func=peek)

    # parse the args and call the selected command function default to help and version
    args = parser.parse_args()
    args.func = getattr(args, 'func', version)
    if args.func == version:
        parser.print_help()
    args.func(args)


def version(args):
    print('pydbx: {}'.format(__version__))
    print('dropbox-sdk-python: {}'.format(dropbox.__version__))
    sys.exit(0)


def status(args):
    dbx = connect_to_dropbox(args)
    account = dbx.users_get_current_account()
    attrs = [x for x in dir(account) if not x.startswith('_') and not callable(getattr(account, x))]
    for attr in attrs:
        print('{:>20}: {}\n'.format(attr, getattr(account, attr), ''))
    sys.exit(0)

def peek(args):
    if not args.queueName:
        return;

    queue = get_queue(args.queueName)
    files = get_files_in_queue(queue)
    for file in files.entries:
        print(json.loads(file)['path_display'])

    if args.flush:
        queue.close()

def ls(args):
    dbx = connect_to_dropbox(args)

    if args.path == '/':
        args.path = ''

    files = dbx.files_list_folder(path=args.path, recursive=args.r)
    while True:
        queue = get_queue(args.pushFilesToQueue) if args.pushFilesToQueue else None
        for entry in files.entries:
            #skip any paths specified in --excludePaths
            if any(map(getattr(entry, 'path_display').startswith, args.excludePaths)):
                print('Excluding {}'.format(getattr(entry, 'path_display')))
                continue

            print('{:>8}  {:>20}  {}'.format(sizeof_fmt(getattr(entry, 'size', 0)),
                                             str(getattr(entry, 'client_modified', '-')),
                                             getattr(entry, 'path_display', '-')))

            # if we have a queue given create if needed and push the files to the queue
            if queue != None and getattr(entry, 'size', None) != None:
                file = FakeFile()
                file.path_display = getattr(entry, 'path_display')
                file.size = getattr(entry, 'size')
                file.server_modified = json_serial(getattr(entry, 'server_modified'))
                file.client_modified = json_serial(getattr(entry, 'client_modified'))
                queue.push(str.encode(json.dumps(file.__dict__)))

        if files.has_more:
            files = dbx.files_list_folder_continue(files.cursor)
        else:
            break

        #write batch
        if queue:
            queue.close()

    #write queue to disk
    if queue:
        queue.close()

def copy_dropbox_file(args, entry, dbx):
    # skip empty files and directories
    if not getattr(entry, 'size', None):
        print('empty file or directory skipping ' + getattr(entry, 'path_display'))
        return

    # skip any paths specified in --excludePaths
    if any(map(getattr(entry, 'path_display').startswith, args.excludePaths)):
        print('Excluding {}'.format(getattr(entry, 'path_display')))
        return

    if args.src_path == '/':
        args.src_path = ''

    local_dir = os.path.dirname(args.dest_path + entry.path_display)
    try:
        if not os.path.exists(local_dir):
            print('Creating directory {}'.format(local_dir))
            os.makedirs(local_dir)
    except Exception as err:
        print(err)
        sys.exit(1)

    src = entry.path_display
    dest = args.dest_path + entry.path_display

    # we have a file so create local directory if necessary then download
    print('Downloading {} to {}' .format(src, dest))

    try:
        dbx.files_download_to_file(path=src, download_path=dest)
    except Exception as err:
        print(err)
        sys.exit(1)
    # set atime/mtime for file
    try:
        modified = entry.server_modified
        cModified = entry.client_modified

        if type(entry.server_modified) is str:
            modified = datetime.strptime(entry.server_modified, '%Y-%m-%dT%H:%M:%S')
        if type(entry.client_modified) is str:
            cModified = datetime.strptime(entry.client_modified, '%Y-%m-%dT%H:%M:%S')

        os.utime(path=dest, times=(modified.timestamp(), cModified.timestamp()))
    except Exception as err:
        print(err)
        sys.exit(1)


def get(args):
    dbx = connect_to_dropbox(args)
    if args.pullFilesFromQueue:
        while True:
            queue = get_queue(args.pullFilesFromQueue)
            entry = queue.pop()

            if not entry:
                break

            # Parse JSON into an object with attributes corresponding to dict keys.
            #https://stackoverflow.com/questions/6578986/how-to-convert-json-data-into-a-python-object
            parsed = json.loads(entry, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
            copy_dropbox_file(args, parsed, dbx)
            queue.close()
    else:
        dbx = connect_to_dropbox(args)
        while True:
            files = files = dbx.files_list_folder(path=args.src_path, recursive=args.r)

            for entry in files.entries:
                copy_dropbox_file(args, entry, dbx)

            if files.has_more:
                files = dbx.files_list_folder_continue(files.cursor)
            else:
                break

def get_files_in_queue(queue):
    result = FakeFileList()

    if not queue:
        print('queue not found')
        return result

    while True:
        file = queue.pop()
        if not file:
            return result;

        result.entries.append(file.decode())

def get_queue(name):
    return FifoDiskQueue(name)

def connect_to_dropbox(args):
    # connect to dropbox
    DROPBOX_TOKEN = args.dropbox_token or os.environ.get('DROPBOX_TOKEN', None)
    if not DROPBOX_TOKEN:
        print('Generate an OAuath 2 Application Developers Access Token from https://dropbox.com/developers/apps')
        sys.exit(1)
    try:
        dbx = dropbox.Dropbox(DROPBOX_TOKEN)
        account = dbx.users_get_current_account()
    except Exception as err:
        print(err)
        sys.exit(1)
    print('Connected to Dropbox as {}\n\n'.format(account.email))
    return dbx


def sizeof_fmt(num, suffix='B'):
    """https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size/"""
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    """https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable"""
    if isinstance(obj, (datetime, date)):
        serial = obj.isoformat()
        return serial
    raise TypeError ("Type %s not serializable" % type(obj))

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
