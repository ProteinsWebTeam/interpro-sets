#!/usr/bin/env python
# -*- coding: utf-8 -*-

import gzip
import os
import re
import sys
from datetime import datetime
from multiprocessing import Pool
from subprocess import Popen, PIPE, DEVNULL
from urllib.request import urlopen

import cx_Oracle


def logger(msg):
    sys.stderr.write(
        "{:%Y-%m-%d %H:%M:%S}: {}\n".format(datetime.now(), msg)
    )
    sys.stderr.flush()


def connect(uri):
    # Format: user/password@host[:port/schema]
    con = cx_Oracle.connect(uri)
    return con, con.cursor()


def init_tables(uri):
    con, cur = connect(uri)

    try:
        cur.execute("DROP TABLE INTERPRO.METHOD_SET")
    except:
        pass

    try:
        cur.execute("DROP TABLE INTERPRO.METHOD_TARGET")
    except:
        pass

    cur.execute(
        """
        CREATE TABLE INTERPRO.METHOD_SET
        (
            METHOD_AC VARCHAR2(25) NOT NULL PRIMARY KEY,
            SET_AC VARCHAR2(25),
            SEQUENCE CLOB NOT NULL,
            CONSTRAINT FK_METHOD_SET$M
              FOREIGN KEY (METHOD_AC)
              REFERENCES INTERPRO.METHOD (METHOD_AC)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE INTERPRO.METHOD_TARGET
        (
            METHOD_AC VARCHAR2(25) NOT NULL,
            TARGET_AC VARCHAR2(25) NOT NULL,
            EVALUE FLOAT NOT NULL,
            DOMAINS CLOB NOT NULL,
            PRIMARY KEY (METHOD_AC, TARGET_AC),
            CONSTRAINT FK_METHOD_TARGET$M
              FOREIGN KEY (METHOD_AC)
              REFERENCES INTERPRO.METHOD (METHOD_AC),
            CONSTRAINT FK_METHOD_TARGET$T
              FOREIGN KEY (TARGET_AC)
              REFERENCES INTERPRO.METHOD (METHOD_AC)
        )
        """
    )


def download(url, dst):
    with urlopen(url) as res, open(dst, "wb") as fh:
        while True:
            data = res.read(1024)
            if data:
                fh.write(data)
            else:
                break


def parse_hmm(filepath, keep_hmm=True):
    entries = {}
    duplicates = set()
    p_acc = re.compile(r"^ACC\s+(\w+)", re.MULTILINE)
    p_name = re.compile(r"^NAME\s+([^\n]+)$", re.MULTILINE)
    p_desc = re.compile(r"^DESC\s+([^\n]+)$", re.MULTILINE)

    hmm = ""
    for line in iterlines(filepath):
        hmm += line

        if line[:2] == "//":
            m1 = p_acc.search(hmm)
            m2 = p_name.search(hmm)
            m3 = p_desc.search(hmm)

            acc = m1.group(1) if m1 else m2.group(1)

            if acc in entries:
                duplicates.add(acc)
            else:
                entries[acc] = {
                    "accession": acc,
                    "name": m2.group(1).rstrip() if m2 else None,
                    "description": m3.group(1).rstrip() if m3 else None,
                    "hmm": hmm if keep_hmm else None
                }

            hmm = ""

    if duplicates:
        logger("WARNING: {} duplicated entries".format(len(duplicates)))

    return entries


def extract(src, dst):
    with open(dst, "wt") as fh:
        for line in iterlines(src):
            fh.write(line)


def iterlines(filepath):
    if filepath.lower().endswith(".gz"):
        fn = gzip.open
    else:
        fn = open

    with fn(filepath, "rt") as fh:
        for line in fh:
            yield line


def read_fasta(filepath):
    seq = ""
    with open(filepath, "rt") as fh:
        next(fh)

        for line in fh:
            seq += line.rstrip()

    return seq


def hmmemit(hmm_file, fasta_file):
    try:
        os.mkdir(os.path.dirname(fasta_file))
    except FileExistsError:
        pass
    except FileNotFoundError as e:
        # Parent directory does not exist
        raise e

    return Popen(
        ["hmmemit", "-c", "-o", fasta_file, hmm_file],
        stdout=DEVNULL, stderr=DEVNULL
    ).wait() == 0


def hmmpress(hmm_db):
    files = [hmm_db + ext for ext in (".h3m", ".h3i", ".h3f", ".h3p")]

    for f in files:
        try:
            os.remove(f)
        except FileNotFoundError:
            pass

    p = Popen(["hmmpress", hmm_db], stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()

    if p.returncode != 0:
        raise RuntimeError("{}\n------\n{}".format(out, err))


def hmmconvert(hmm_file):
    hmm = ""
    pop = Popen(
        ["hmmconvert", hmm_file], stdout=PIPE, stderr=DEVNULL
    )
    for line in pop.stdout:
        hmm += line.decode("utf-8")

    out, err = pop.communicate()
    return hmm


def _hmmconvert(args):
    acc, hmm_file = args
    return acc, hmmconvert(hmm_file)


def _batch(func, jobs, processes):
    if processes > 1:
        with Pool(processes) as pool:
            for res in pool.imap_unordered(func, jobs):
                yield res
    else:
        for res in map(func, jobs):
            yield res


def hmmscan(acc, fasta_file, hmm_db):
    tab_file = fasta_file[:-2] + 'tab'
    out_file = fasta_file[:-2] + 'out'

    with open(out_file, 'wt') as fh:
        # (?) option for --cut_ga and -E
        Popen(
            ["hmmscan", "--domtblout", tab_file, hmm_db, fasta_file],
            stdout=fh, stderr=DEVNULL
        ).wait()

    return acc, fasta_file, out_file, tab_file


def _hmmscan(args):
    hmmscan(*args)


def batch_hmmconvert(jobs, processes=1):
    return _batch(_hmmconvert, jobs, processes)


def batch_hmmscan(jobs, processes=1):
    return _batch(_hmmscan, jobs, processes)


def parse_block(fh, line):
    block = []
    while line or len(block) < 4:
        block.append(line)
        line = next(fh).strip()

    i = 0
    for i, line in enumerate(block):
        if len(line.split()) == 4:
            break

    target = block[i].split()[2]
    query = block[i+2].split()[2]

    return target, query


def _parse_hmmscan_alignments(filepath):
    domains = []
    target = ""
    query = ""
    # p_dom = re.compile(
    #     r"== domain (\d+)\s+score: ([\d.]+) bits;\s*conditional E-value: ([\d.\-e]+)"
    # )
    n_blank = 0
    with open(filepath, "rt") as fh:
        for line in fh:
            line = line.strip()
            if line:
                n_blank = 0
            else:
                n_blank += 1

            if line.startswith("== domain"):
                # new domain: flush previous one
                if target:
                    domains.append({"target": target, "query": query})
                    target = ""
                    query = ""

                t, q = parse_block(fh, next(fh).strip())
                target += t
                query += q

            elif line.startswith(">>"):
                # new complete sequence: flush previous domain
                if target:
                    domains.append({"target": target, "query": query})
                    target = ""
                    query = ""
            elif target:
                if line:
                    t, q = parse_block(fh, next(fh).strip())
                    target += t
                    query += q
                elif n_blank == 2:
                    domains.append({"target": target, "query": query})
                    target = ""
                    query = ""

    return domains


def parse_hmmscan_results(out_file, tab_file):
    alignments = _parse_hmmscan_alignments(out_file)

    targets = {}
    i = 0

    for line in iterlines(tab_file):
        if line[0] == "#":
            continue

        cols = re.split(r"\s+", line.rstrip(), maxsplit=22)

        # Pfam entries end with a mark followed by a number
        acc = cols[1].split(".")

        if acc == "-":
            # Panther accessions are under the `target_name` column
            acc = cols[0]

        if acc in targets:
            t = targets[acc]
        else:
            t = targets[acc] = {
                "accession": acc,
                "tlen": int(cols[2]),
                "qlen": int(cols[5]),

                # full sequence
                "evalue": float(cols[6]),
                "score": float(cols[7]),
                "bias": float(cols[8]),

                "domains": []
            }

        t["domains"].append({
            # this domain

            # conditional E-value
            "cevalue": float(cols[11]),
            # independent E-value
            "ievalue": float(cols[12]),
            "score": float(cols[13]),
            "bias": float(cols[14]),

            "coordinates": {
                # target (as we scan an HMM DB)
                "hmm": {
                    "start": int(cols[15]),
                    "end": int(cols[16])
                },
                # query
                "ali": {
                    "start": int(cols[17]),
                    "end": int(cols[18])
                },
                "env": {
                    "start": int(cols[19]),
                    "end": int(cols[20])
                },
            },
            "sequences": alignments[i]
        })
        i += 1

    return list(targets.values())
