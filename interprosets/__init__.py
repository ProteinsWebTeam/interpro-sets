#!/usr/bin/env python
# -*- coding: utf-8 -*-


def cli():
    import argparse
    import os
    import tempfile

    from . import cdd, panther, pfam, pirsf, utils

    parser = argparse.ArgumentParser(
        description="Sets/Collections in InterPro"
    )

    try:
        uri = os.environ["INTERPRO_URI"]
    except KeyError:
        parser.error("Please define the INTERPRO_URI environment variable")

    dir_arg = {
        "help": "temporary directory",
        "default": tempfile.gettempdir()
    }
    proc_arg = {
        "help": "number of processes (default: 1)",
        "type": int,
        "default": 1,
        "dest": "processes"
    }

    parser.add_argument("--dir",
                        help="temporary directory")
    parser.add_argument("-p", dest="processes", type=int, default=1,
                        help="number of processes (default: 1)")

    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True
    subparsers.add_parser("init", help="(re)create tables")

    _parser = subparsers.add_parser(
        "cdd", help="CDD profile-profile alignments with COMPASS"
    )
    _parser.add_argument("--dir", **dir_arg)
    _parser.add_argument("-p", **proc_arg)
    _parser.add_argument("--sequences",
                         help="FASTA file of representative sequences")
    _parser.add_argument("--links",
                         help="list of superfamilies and their domain models")

    _parser = subparsers.add_parser(
        "panther", help="PANTHER profile-profile alignments with HMMSCAN"
    )
    _parser.add_argument("--dir", **dir_arg)
    _parser.add_argument("-p", **proc_arg)
    _parser.add_argument("--books",
                         help="directory of 'books' (protein families)",
                         required=True)

    _parser = subparsers.add_parser(
        "pfam", help="Pfam profile-profile alignments with HMMSCAN"
    )
    _parser.add_argument("--dir", **dir_arg)
    _parser.add_argument("-p", **proc_arg)
    _parser.add_argument("--hmm", help="Pfam-A HMM file")
    _parser.add_argument("--clans", help="Pfam clans TSV file")

    _parser = subparsers.add_parser(
        "pirsf", help="PIRFS profile-profile alignments with HMMSCAN"
    )
    _parser.add_argument("--dir", **dir_arg)
    _parser.add_argument("-p", **proc_arg)
    _parser.add_argument("--hmm", help="PIRSF HMM file", required=True)
    _parser.add_argument("--info", help="pirsfinfo.dat file")

    args = parser.parse_args()

    if args.command == "init":
        utils.init_tables(uri)
    else:
        os.makedirs(args.dir, exist_ok=True)

        with tempfile.TemporaryDirectory(dir=args.dir) as tmpdir:
            if args.command == "cdd":
                cdd.run(uri,
                        cdd_masters=args.sequences,
                        links=args.links,
                        processes=args.processes,
                        tmpdir=tmpdir)

            elif args.command == "panther":
                panther.run(uri, args.books,
                            tmpdir=tmpdir,
                            processes=args.processes)

            elif args.command == "pfam":
                pfam.run(uri,
                         hmm_db=args.hmm,
                         clans_tsv=args.clans,
                         processes=args.processes,
                         tmpdir=tmpdir)

            elif args.command == "pirsf":
                pirsf.run(uri, args.hmm,
                          pirsfinfo=args.info,
                          tmpdir=tmpdir,
                          processes=args.processes)

            size = 0
            for root, dirs, files in os.walk(tmpdir):
                for f in files:
                    size += os.path.getsize(os.path.join(root, f))

            utils.logger("temporary files: {} bytes".format(size))
