#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os
import shutil
import tempfile


def main():
    parser = argparse.ArgumentParser(
        description="Sets/Collections in InterPro"
    )
    parser.add_subparsers()
    parser.add_argument("--uri",
                        help="database connection string "
                             "(driver:user/password@host:port/database)",
                        required=True)
    parser.add_argument("--dir",
                        help="working directory "
                             "(default: temporary directory)")
    parser.add_argument("-p", dest="processes", type=int, default=1,
                        help="number of processes (default: 1)")

    parsers = parser.add_subparsers()
    _parser = parsers.add_parser("init", help="(re)create tables")
    _parser = parsers.add_parser("cdd", help="")
    _parser = parsers.add_parser("panther", help="")
    _parser = parsers.add_parser("pfam", help="")
    _parser = parsers.add_parser("pirsf", help="")

    args = parser.parse_args()

    if args.dir:
        workdir = args.dir
        delete_dir = False
        try:
            os.makedirs(workdir)
        except FileExistsError:
            pass
    else:
        workdir = tempfile.mkdtemp()
        delete_dir = True

    processes = args.processes
    if processes < 1:
        processes = 1

    if delete_dir:
        shutil.rmtree(workdir)
