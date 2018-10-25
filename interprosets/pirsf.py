#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
import re
from tempfile import mkstemp

import cx_Oracle

from . import utils

INFO = "ftp://ftp.pir.georgetown.edu/databases/pirsf/pirsfinfo.dat"
DBCODE = "U"


def parse_dat(filepath):
    p1 = re.compile(r"\[Parent=(PIRSF\d+)\]", re.I)
    p2 = re.compile(r">(PIRSF\d+)\s+\([\w/.]+\)\s*(.*)", re.I)

    families = {}
    for line in utils.iterlines(filepath):
        if line[0] == ">":
            m = p1.search(line)
            if m:
                parent = m.group(1)
                line = line[:m.start()]
            else:
                parent = None

            m = p2.search(line)
            acc = m.group(1)
            #description = m.group(2).strip()

            families[acc] = parent

    return families


def run(uri, sf_hmm_all, pirsfinfo=None, processes=1, tmpdir=None):
    if pirsfinfo is None:
        fd, pirsfinfo = mkstemp(suffix=os.path.basename(INFO), dir=tmpdir)
        os.close(fd)

        utils.download(INFO, pirsfinfo)

    utils.logger("parse sets")
    families = parse_dat(pirsfinfo)

    fd, hmm_db = mkstemp(dir=tmpdir)
    os.close(fd)

    jobs = []
    dirs = []
    utils.logger("run hmmemit")
    with open(hmm_db, "wt") as fh:
        for acc, e in utils.parse_hmm(sf_hmm_all).items():
            fh.write(e["hmm"])

            fd, hmm_file = mkstemp(dir=tmpdir)
            os.close(fd)

            with open(hmm_file, "wt") as fh2:
                fh2.write(e["hmm"])

            _dir = os.path.join(tmpdir, acc[:8])
            try:
                os.mkdir(_dir)
            except FileExistsError:
                pass
            else:
                dirs.append(_dir)

            fa_file = os.path.join(_dir, acc + ".fa")
            utils.hmmemit(hmm_file, fa_file)
            jobs.append((acc, fa_file, hmm_db))

    utils.logger("compress HMM database")
    utils.hmmpress(hmm_db)

    con = cx_Oracle.connect(uri)
    utils.prepare_tables(con, DBCODE)

    cur1 = con.cursor()
    cur2 = con.cursor()
    cur2.setinputsizes(evalue=cx_Oracle.NATIVE_FLOAT)
    cnt = 0
    data1 = []
    data2 = []
    utils.logger("run hmmscan: {:>10} / {}".format(cnt, len(jobs)))
    for acc, fa_file, out_file, tab_file in utils.batch_hmmscan(jobs, processes):
        sequence, _ = utils.read_fasta(fa_file)
        targets = utils.parse_hmmscan_results(out_file, tab_file)

        data1.append((
            acc,
            DBCODE,
            families.get(acc),
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
            utils.logger("run hmmscan: {:>10} / {}".format(cnt, len(jobs)))

    utils.logger("run hmmscan: {:>10} / {}".format(cnt, len(jobs)))

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

