#!/usr/bin/env python
# -*- coding: utf-8 -*-

import gzip
import os
import re
import sys
from datetime import datetime
from multiprocessing.dummy import Pool
from subprocess import Popen, PIPE, DEVNULL
from urllib.request import urlopen

import cx_Oracle

INSERT_SIZE = 1000


def batch_compass(jobs, processes=1):
    return _batch(_compass, jobs, processes)


def batch_hmmconvert(jobs, processes=1):
    return _batch(_hmmconvert, jobs, processes)


def batch_hmmscan(jobs, processes=1):
    return _batch(_hmmscan, jobs, processes)


def compass(fasta_file, profile_db):
    out_file = fasta_file[:-2] + "out"
    cmd = [
        "compass_vs_db",
        "-i", fasta_file,
        "-d", profile_db,
        "-o", out_file
    ]
    _exec_shell(" ".join(cmd)).wait()
    return out_file


def download(url, dst):
    with urlopen(url) as res, open(dst, "wb") as fh:
        while True:
            data = res.read(1024)
            if data:
                fh.write(data)
            else:
                break


def extract(src, dst):
    with open(dst, "wt") as fh:
        for line in iterlines(src):
            fh.write(line)


def hmmconvert(hmm_file):
    cmd = "hmmconvert " + hmm_file
    p = _exec_shell(cmd, PIPE, DEVNULL)
    hmm = ""
    for line in p.stdout:
        hmm += line.decode("utf-8")

    out, err = p.communicate()
    return hmm


def hmmemit(hmm_file, fasta_file):
    cmd = ["hmmemit", "-c", "-o", fasta_file, hmm_file]
    return _exec_shell(" ".join(cmd)).wait() == 0


def hmmpress(hmm_db):
    files = [hmm_db + ext for ext in (".h3m", ".h3i", ".h3f", ".h3p")]

    for f in files:
        try:
            os.remove(f)
        except FileNotFoundError:
            pass

    cmd = "hmmpress " + hmm_db
    p = _exec_shell(cmd, PIPE, PIPE)
    out, err = p.communicate()

    if p.returncode != 0:
        raise RuntimeError("{}\n------\n{}".format(out, err))


def hmmscan(fasta_file, hmm_db):
    tab_file = fasta_file[:-2] + 'tab'
    out_file = fasta_file[:-2] + 'out'

    cmd = ["hmmscan", "--domtblout", tab_file, hmm_db, fasta_file]

    with open(out_file, 'wt') as fh:
        # (?) option for --cut_ga and -E
        _exec_shell(" ".join(cmd), fh).wait()

    return out_file, tab_file


def init_tables(uri):
    con = cx_Oracle.connect(uri)
    cur = con.cursor()

    try:
        cur.execute("DROP TABLE INTERPRO.METHOD_SET")
    except:
        pass

    try:
        cur.execute("DROP TABLE INTERPRO.METHOD_SCAN")
    except:
        pass

    # cur.execute(
    #     """
    #     CREATE TABLE INTERPRO.METHOD_SET
    #     (
    #         METHOD_AC VARCHAR2(25) NOT NULL,
    #         SET_AC VARCHAR2(25),
    #         SEQUENCE CLOB NOT NULL,
    #         CONSTRAINT PK_METHOD_SET PRIMARY KEY (METHOD_AC),
    #         CONSTRAINT FK_METHOD_SET$M
    #           FOREIGN KEY (METHOD_AC)
    #           REFERENCES INTERPRO.METHOD (METHOD_AC)
    #     )
    #     """
    # )

    cur.execute(
        """
        CREATE TABLE INTERPRO.METHOD_SET
        (
            METHOD_AC VARCHAR2(25) NOT NULL,
            DBCODE CHAR(1) NOT NULL,
            SET_AC VARCHAR2(25),
            SEQUENCE CLOB NOT NULL,
            CONSTRAINT PK_METHOD_SET PRIMARY KEY (METHOD_AC)
        )
        """
    )

    # cur.execute(
    #     """
    #     CREATE TABLE INTERPRO.METHOD_TARGET
    #     (
    #         METHOD_AC VARCHAR2(25) NOT NULL,
    #         TARGET_AC VARCHAR2(25) NOT NULL,
    #         EVALUE BINARY_DOUBLE NOT NULL,
    #         DOMAINS CLOB NOT NULL,
    #         CONSTRAINT PK_METHOD_TARGET PRIMARY KEY (METHOD_AC, TARGET_AC),
    #         CONSTRAINT FK_METHOD_TARGET$M
    #           FOREIGN KEY (METHOD_AC)
    #           REFERENCES INTERPRO.METHOD (METHOD_AC),
    #         CONSTRAINT FK_METHOD_TARGET$T
    #           FOREIGN KEY (TARGET_AC)
    #           REFERENCES INTERPRO.METHOD (METHOD_AC)
    #     )
    #     """
    # )

    cur.execute(
        """
        CREATE TABLE INTERPRO.METHOD_SCAN
        (
            QUERY_AC VARCHAR2(25) NOT NULL,
            TARGET_AC VARCHAR2(25) NOT NULL,
            EVALUE BINARY_DOUBLE NOT NULL,
            EVALUE_STR VARCHAR2(10) NOT NULL,
            DOMAINS CLOB NOT NULL,
            CONSTRAINT PK_METHOD_SCAN PRIMARY KEY (QUERY_AC, TARGET_AC)
        )
        """
    )


def iterlines(filepath):
    if filepath.lower().endswith(".gz"):
        fn = gzip.open
    else:
        fn = open

    with fn(filepath, "rt") as fh:
        for line in fh:
            yield line


def logger(msg):
    sys.stderr.write(
        "{:%Y-%m-%d %H:%M:%S}: {}\n".format(datetime.now(), msg)
    )
    sys.stderr.flush()


def mk_compass_db(files_list, profile_db):
    cmd = ["mk_compass_db", "-i", files_list, "-o", profile_db]
    return _exec_shell(" ".join(cmd)).wait() == 0


def parse_compass_results(out_file):
    p1 = re.compile(r"length\s*=\s*(\d+)")
    p2 = re.compile(r"Evalue\s*=\s*([\d.e\-]+)")

    targets = {}
    block = 0
    query_id = None
    query_seq = ""
    target_id = None
    target_seq = ""
    length = None
    evalue = None
    evalue_str = None
    pos_start = None

    it = iterlines(out_file)
    for line in it:
        line = line.rstrip()
        if line.startswith("Subject="):
            """
            Format:
            Subject= cd154/cd15468.fa
            length=413	filtered_length=413	Neff=1.000
            Smith-Waterman score = 254	Evalue = 3.36e-16

            (the path after "Subject=" might be truncated)
            """
            if target_id:
                targets[target_id] = {
                    "id": target_id,
                    "evalue": evalue,
                    "evaluestr": evalue_str,
                    "length": length,
                    "start": pos_start,
                    "end": pos_start + length - 1,
                    "sequences": {
                        "query": query_seq,
                        "target": target_seq
                    }
                }

            query_id = None
            query_seq = None
            target_id = None
            target_seq = None

            line = next(it)
            length = int(p1.match(line).group(1))

            line = next(it)
            evalue_str = p2.search(line).group(1)
            try:
                evalue= float(evalue_str)
            except ValueError:
                evalue = 0

            block = 1
        elif line.startswith("Parameters:"):
            # Footer: end of results
            break
        elif not block:
            continue
        elif line:
            """
            First block:
            gnl|CDD|271233   1      PSFIPGPT==TPKGCTRIPSFSLSDTHWCYTHNVILSGCQDHSKSNQYLSLGVIKTNSDG
            CONSENSUS_1      1      PSFIPGPT==TPKGCTRIPSFSLSDTHWCYTHNVILSGCQDHSKSNQYLSLGVIKTNSDG
                                    P++IP+ T      C+R PSF++S+  + YT+ V  ++CQDH +  +Y+++GVI+ ++ G
            CONSENSUS_2      1      PNLIPADTGLLSGECVRQPSFAISSGIYAYTYLVRKGSCQDHRSLYRYFEVGVIRDDGLG
            gnl|CDD|271230   1      PNLIPADTGLLSGECVRQPSFAISSGIYAYTYLVRKGSCQDHRSLYRYFEVGVIRDDGLG

            (following blocks do not have the start position between the ID and the sequence)
            """
            query = line.split()
            next(it)
            next(it)
            next(it)
            target = next(it).split()

            if block == 1:
                query_id = query[0]
                pos_start = int(query[1])
                query_seq = query[2]
                target_id = target[0]
                target_seq = target[2]
            else:
                query_seq += query[1]
                target_seq += target[1]

            block += 1

    targets[target_id] = {
        "id": target_id,
        "evalue": evalue,
        "evaluestr": evalue_str,
        "length": length,
        "start": pos_start,
        "end": pos_start + length - 1,
        "sequences": {
            "query": query_seq,
            "target": target_seq
        }
    }

    return list(targets.values())


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


def parse_hmmscan_results(out_file, tab_file):
    alignments = _parse_hmmscan_alignments(out_file)

    targets = {}
    i = 0

    for line in iterlines(tab_file):
        if line[0] == "#":
            continue

        cols = re.split(r"\s+", line.rstrip(), maxsplit=22)

        # Pfam entries end with a mark followed by a number
        acc = cols[1].split(".")[0]

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
                "evaluestr": cols[6],
                "score": float(cols[7]),
                "bias": float(cols[8]),

                "domains": []
            }

        t["domains"].append({
            # this domain

            # conditional E-value
            "cevalue": float(cols[11]),
            "cevaluestr": cols[11],
            # independent E-value
            "ievalue": float(cols[12]),
            "ievaluestr": cols[12],
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


def read_fasta(filepath, reo=None):
    it = iterlines(filepath)
    line = next(it)
    if reo:
        m = reo.match(line)
    else:
        m = None

    seq = ""
    for line in it:
        seq += line.rstrip()

    return seq, m


def _batch(func, jobs, processes):
    if processes > 1:
        with Pool(processes) as pool:
            for res in pool.imap_unordered(func, jobs):
                yield res
    else:
        for res in map(func, jobs):
            yield res


def _compass(args):
    acc, fasta_file, profile_db = args
    out_file = compass(fasta_file, profile_db)
    return acc, fasta_file, out_file


def _exec_shell(cmd, stdout=DEVNULL, stderr=DEVNULL):
    return Popen(cmd, shell=True, stdout=stdout, stderr=stderr)


def _hmmconvert(args):
    acc, hmm_file = args
    return acc, hmmconvert(hmm_file)


def _hmmscan(args):
    acc, fasta_file, hmm_db = args
    out_file, tab_file = hmmscan(fasta_file, hmm_db)
    return acc, fasta_file, out_file, tab_file


def _parse_block(fh, line):
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

                t, q = _parse_block(fh, next(fh).strip())
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
                    t, q = _parse_block(fh, line)
                    target += t
                    query += q
                elif n_blank == 2:
                    domains.append({"target": target, "query": query})
                    target = ""
                    query = ""

    return domains


def prepare_tables(con, dbcode):
    cur = con.cursor()

    cur.execute(
        """
        DELETE FROM INTERPRO.METHOD_SCAN
        WHERE QUERY_AC IN (
          SELECT METHOD_AC FROM INTERPRO.METHOD_SET
          WHERE DBCODE = :1
        )
        """,
        (dbcode,)
    )

    cur.execute(
        """
        DELETE FROM INTERPRO.METHOD_SET
        WHERE DBCODE = :1
        """,
        (dbcode,)
    )

    con.commit()
    cur.close()
