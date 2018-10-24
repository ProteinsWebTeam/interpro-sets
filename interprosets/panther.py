#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
import re
from tempfile import gettempdir, mkstemp

import cx_Oracle

from . import utils

DBCODE = "V"


def find_hmm_files(path):
    entries = {}
    for root, dirs, files in os.walk(path):
        for f in files:
            if f == "hmmer.hmm":
                head, tail = os.path.split(root)
                if tail.startswith("PTHR"):
                    acc = tail
                else:
                    # Pattern: SF\d+
                    acc = os.path.split(head)[1] + ':' + tail

                entries[acc] = os.path.join(root, f)

    return entries


def run(uri, books, tmpdir=gettempdir(), processes=1):
    os.makedirs(tmpdir, exist_ok=True)

    fd, hmm_db = mkstemp(dir=tmpdir)
    os.close(fd)

    utils.logger("find profile files")
    jobs = list(find_hmm_files(books).items())
    jobs2 = []

    # hmmconvert: convert profile files to HMMER3 files
    dirs = []
    with open(hmm_db, "wt") as fh:
        for acc, hmm in utils.batch_hmmconvert(jobs, processes):
            # Add the accession to the HMM so we can link alignments to entries
            hmm = re.sub(
                r"^(NAME\s+)[\w.]+$",
                r"\1{}".format(acc),
                hmm,
                flags=(re.I | re.MULTILINE)
            )

            # add HMM to HMM database
            fh.write(hmm)

            # run hmmemit
            fd, hmm_file = mkstemp(dir=tmpdir)
            os.close(fd)

            with open(hmm_file, "wt") as fh2:
                fh2.write(hmm)

            _dir = os.path.join(tmpdir, acc[:7])
            try:
                os.mkdir(_dir)
            except FileExistsError:
                pass
            else:
                dirs.append(_dir)

            fa_file = os.path.join(_dir, acc + ".fa")
            utils.hmmemit(hmm_file, fa_file)
            os.remove(hmm_file)

            jobs2.append((acc, fa_file, hmm_db))

            if not len(jobs2) % 1000:
                utils.logger("run hmmemit: {:>6} / {}".format(
                    len(jobs2), len(jobs)
                ))

    utils.logger("run hmmemit: {:>6} / {}".format(
        len(jobs2), len(jobs)
    ))

    jobs = []

    utils.logger("compress HMM database")
    utils.hmmpress(hmm_db)

    con = cx_Oracle.connect(uri)
    cur1 = con.cursor()
    cur2 = con.cursor()
    cur2.setinputsizes(evalue=cx_Oracle.NATIVE_FLOAT)
    cnt = 0
    data1 = []
    data2 = []
    for acc, fa_file, out_file, tab_file in utils.batch_hmmscan(jobs2, processes):
        sequence, _ = utils.read_fasta(fa_file)
        targets = utils.parse_hmmscan_results(out_file, tab_file)

        os.remove(fa_file)
        os.remove(out_file)
        os.remove(tab_file)

        data1.append((
            acc,
            DBCODE,
            acc.split(":")[0] if ":" in acc else None,
            sequence
        ))

        if len(data1) == utils.INSERT_SIZE:
            cur1.executemany(
                """
                INSERT INTO INTERPRO.METHOD_SET
                VALUES (:1, :2, :3, :4)
                """,
                data1
            )
            data1 = []

        for t in targets:
            if acc == t["accession"]:
                continue

            domains = []
            for dom in t["domains"]:
                domains.append({
                    "query": dom["sequences"]["query"],
                    "target": dom["sequences"]["target"],
                    "ievalue": dom["ievalue"],
                    "start": dom["coordinates"]["ali"]["start"],
                    "end": dom["coordinates"]["ali"]["end"],
                })

            data2.append({
                "query_ac": acc,
                "target_ac": t["accession"],
                "evalue": t["evalue"],
                "evaluestr": t["evaluestr"],
                "domains": json.dumps(domains)
            })

            if len(data2) == utils.INSERT_SIZE:
                cur2.executemany(
                    """
                    INSERT INTO INTERPRO.METHOD_SCAN
                    VALUES (
                      :query_ac, :target_ac, :evalue, :evaluestr, :domains
                    )
                    """,
                    data2
                )
                data2 = []

        cnt += 1
        if not cnt % 1000:
            utils.logger("run hmmscan: {:>10} / {}".format(cnt, len(jobs2)))

    utils.logger("run hmmscan: {:>10} / {}".format(cnt, len(jobs2)))

    if data1:
        cur1.executemany(
            """
            INSERT INTO INTERPRO.METHOD_SET
            VALUES (:1, :2, :3, :4)
            """,
            data1
        )

    if data2:
        cur2.executemany(
            """
            INSERT INTO INTERPRO.METHOD_SCAN
            VALUES (:query_ac, :target_ac, :evalue, :evaluestr, :domains)
            """,
            data2
        )

    con.commit()
    cur1.close()
    cur2.close()
    con.close()

    os.remove(hmm_db)
    for ext in ("h3f", "h3i", "h3m", "h3p"):
        try:
            os.remove(hmm_db + "." + ext)
        except FileNotFoundError:
            pass
    for d in dirs:
        os.rmdir(d)
