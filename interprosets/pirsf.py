#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
import re
from tempfile import gettempdir, mkstemp

import cx_Oracle

from . import utils

INFO = "ftp://ftp.pir.georgetown.edu/databases/pirsf/pirsfinfo.dat"


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


def run(uri, sf_hmm_all, pirsfinfo=None, tmpdir=gettempdir(), processes=1):
    os.makedirs(tmpdir, exist_ok=True)

    if pirsfinfo is None:
        rm_pirsfinfo = True
        fd, pirsfinfo = mkstemp(suffix=os.path.basename(INFO), dir=tmpdir)
        os.close(fd)

        utils.download(INFO, pirsfinfo)
    else:
        rm_pirsfinfo = False

    utils.logger("parse sets")
    families = parse_dat(pirsfinfo)
    if rm_pirsfinfo:
        os.remove(pirsfinfo)

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
            os.remove(hmm_file)
            jobs.append((acc, fa_file, hmm_db))

    utils.logger("compress HMM database")
    utils.hmmpress(hmm_db)

    con = cx_Oracle.connect(uri)
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

        os.remove(fa_file)
        os.remove(out_file)
        os.remove(tab_file)

        data1.append((
            acc,
            families.get(acc),
            sequence
        ))

        if len(data1) == utils.INSERT_SIZE:
            cur1.executemany(
                """
                INSERT INTO INTERPRO.METHOD_SET
                VALUES (:1, :2, :3)
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
                "domains": json.dumps(domains)
            })

            if len(data2) == utils.INSERT_SIZE:
                cur2.executemany(
                    """
                    INSERT INTO INTERPRO.METHOD_SCAN
                    VALUES (:query_ac, :target_ac, :evalue, :domains)
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
            VALUES (:1, :2, :3)
            """,
            data1
        )

    if data2:
        cur2.executemany(
            """
            INSERT INTO INTERPRO.METHOD_SCAN
            VALUES (:query_ac, :target_ac, :evalue, :domains)
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
