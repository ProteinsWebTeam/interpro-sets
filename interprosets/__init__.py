#!/usr/bin/env python
# -*- coding: utf-8 -*-


def main():
    import argparse
    import utils

    parser = argparse.ArgumentParser(
        description="Sets/Collections in InterPro"
    )

    uri_arg = {
        "help": "database connection string "
                "(user/password@host:port/database)",
        "required": True
    }
    dir_arg = {
        "help": "temporary directory"
    }
    proc_arg = {
        "help": "number of processes (default: 1)",
        "type": int,
        "default": 1
    }

    parser.add_argument("--dir",
                        help="temporary directory")
    parser.add_argument("-p", dest="processes", type=int, default=1,
                        help="number of processes (default: 1)")

    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True
    _parser = subparsers.add_parser("init", help="(re)create tables")
    _parser.add_argument("--uri", **uri_arg)

    _parser = subparsers.add_parser("cdd", help="")
    _parser.add_argument("--uri", **uri_arg)
    _parser.add_argument("--dir", **dir_arg)
    _parser.add_argument("-p", **proc_arg)

    _parser = subparsers.add_parser("panther", help="")
    _parser.add_argument("--uri", **uri_arg)
    _parser.add_argument("--dir", **dir_arg)
    _parser.add_argument("-p", **proc_arg)

    _parser = subparsers.add_parser("pfam", help="")
    _parser.add_argument("--uri", **uri_arg)
    _parser.add_argument("--dir", **dir_arg)
    _parser.add_argument("-p", **proc_arg)

    _parser = subparsers.add_parser("pirsf", help="")
    _parser.add_argument("--uri", **uri_arg)
    _parser.add_argument("--dir", **dir_arg)
    _parser.add_argument("-p", **proc_arg)

    args = parser.parse_args()

    if args.command == "init":
        utils.init_tables(args.uri)
    elif args.command == "cdd":
        pass
    elif args.command == "panther":
        pass
    elif args.command == "pfam":
        pass
    elif args.command == "pirsf":
        pass


if __name__ == '__main__':
    main()


