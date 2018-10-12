#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
from tempfile import gettempdir, mkstemp

import cx_Oracle

from . import utils

HMM = "ftp://ftp.ebi.ac.uk/pub/databases/Pfam/current_release/Pfam-A.hmm.gz"
CLANS = "ftp://ftp.ebi.ac.uk/pub/databases/Pfam/current_release/Pfam-A.clans.tsv.gz"
DBCODE = "H"


def parse_clans(filepath, entries):
    for line in utils.iterlines(filepath):
        cols = line.rstrip().split("\t")
        fam_id = cols[0]
        clan_id = cols[1]
        if clan_id and fam_id in entries:
            entries[fam_id]["parent"] = clan_id


def run(uri, hmm_db=None, clans_tsv=None, processes=1, tmpdir=gettempdir()):
    os.makedirs(tmpdir, exist_ok=True)

    if hmm_db is None:
        rm_hmm_db = True
        fd, hmm_db = mkstemp(suffix=os.path.basename(HMM), dir=tmpdir)
        os.close(fd)

        utils.download(HMM, hmm_db)
    else:
        rm_hmm_db = False

    if hmm_db.endswith(".gz"):
        fd, _hmm_db = mkstemp(dir=tmpdir)
        os.close(fd)

        utils.extract(hmm_db, _hmm_db)
        if rm_hmm_db:
            os.remove(hmm_db)

        hmm_db = _hmm_db
        rm_hmm_db = True

    utils.logger("parse HMMs")
    entries = utils.parse_hmm(hmm_db, keep_hmm=True)

    if clans_tsv is None:
        rm_clans_tsv = True
        fd, clans_tsv = mkstemp(suffix=os.path.basename(CLANS), dir=tmpdir)
        os.close(fd)

        utils.download(CLANS, clans_tsv)
    else:
        rm_clans_tsv = False

    utils.logger("parse clans")
    parse_clans(clans_tsv, entries)

    if rm_clans_tsv:
        os.remove(clans_tsv)

    utils.logger("run hmmemit")
    jobs = []
    dirs = []
    for acc, e in entries.items():
        fd, hmm_file = mkstemp(dir=tmpdir)
        os.close(fd)

        with open(hmm_file, "wt") as fh:
            fh.write(e["hmm"])

        _dir = os.path.join(tmpdir, acc[:5])
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

        # os.remove(fa_file)
        os.remove(out_file)
        os.remove(tab_file)

        e = entries[acc]
        data1.append((
            acc,
            DBCODE,
            e.get("parent"),
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
            VALUES (:1, :2, :3, :4)
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

    # if rm_hmm_db:
    #     os.remove(hmm_db)
    #     for ext in ("h3f", "h3i", "h3m", "h3p"):
    #         try:
    #             os.remove(hmm_db + "." + ext)
    #         except FileNotFoundError:
    #             pass
    #
    # for d in dirs:
    #     os.rmdir(d)
