#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
from tempfile import gettempdir, mkstemp

from . import utils

HMM = "ftp://ftp.ebi.ac.uk/pub/databases/Pfam/current_release/Pfam-A.hmm.gz"
CLANS = "ftp://ftp.ebi.ac.uk/pub/databases/Pfam/current_release/Pfam-A.clans.tsv.gz"


def parse_clans(filepath, entries):
    for line in utils.iterlines(filepath):
        cols = line.rstrip().split("\t")
        fam_id = cols[0]
        clan_id = cols[1]
        if clan_id and fam_id in entries:
            entries[fam_id]["parent"] = clan_id


def run(uri, dbcode, hmm_db=None, clans_tsv=None, processes=1,
        tmpdir=gettempdir(), remove=True, chunk_size=100000):

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

    utils.logger("run hmmemit")
    jobs = []
    dirs = set()
    files = set()
    for acc, e in entries.items():
        fd, hmm_file = mkstemp(dir=tmpdir)
        os.close(fd)

        with open(hmm_file, "wt") as fh:
            fh.write(e["hmm"])

        fa_dir = os.path.join(tmpdir, acc[:5])
        fa_file = os.path.join(fa_dir, acc + ".fa")

        dirs.add(fa_dir)
        files.add(fa_file)

        if utils.hmmemit(hmm_file, fa_file):
            jobs.append((acc, fa_file, hmm_db))

        os.remove(hmm_file)

    utils.logger("prepare HMM database")
    utils.hmmpress(hmm_db)

    con, cur = utils.connect(uri)
    cnt = 0
    data1 = []
    data2 = []
    for acc, fa_file, out_file, tab_file in utils.batch_hmmscan(jobs, processes):
        files.add(out_file)
        files.add(tab_file)

        sequence = utils.read_fasta(fa_file)
        targets = utils.parse_hmmscan_results(out_file, tab_file)

        e = entries[acc]
        data1.append((
            acc,
            e["name"],
            e.get("parent"),
            dbcode,
            sequence
        ))

        if len(data1) == chunk_size:
            cur.executemany(
                """
                INSERT INTO INTERPRO.METHOD_SET
                VALUES (:1, :2, :3, :4, :5)
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

            data2.append((
                acc,
                t["accession"],
                t["evalue"],
                json.dumps(domains)
            ))

            if len(data2) == chunk_size:
                cur.executemany(
                    """
                    INSERT INTO INTERPRO.METHOD_TARGET
                    VALUES (:1, :2, :3, :4)
                    """,
                    data2
                )
                data2 = []

        cnt += 1
        if not cnt % 1000:
            utils.logger("run hmmscan: {:>10} / {}".format(cnt, len(jobs)))

    if data1:
        cur.executemany(
            """
            INSERT INTO INTERPRO.SET_MEMBER
            VALUES (:1, :2, :3, :4, :5)
            """,
            data1
        )

    if data2:
        cur.executemany(
            """
            INSERT INTO INTERPRO.METHOD_TARGET
            VALUES (:1, :2, :3, :4)
            """,
            data2
        )
        data2 = []

    cur.close()
    con.commit()
    con.close()

    if rm_hmm_db:
        os.remove(hmm_db)

    if rm_clans_tsv:
        os.remove(clans_tsv)

    if remove:
        utils.logger("remove files")
        for f in files:
            os.remove(f)

        for d in dirs:
            os.rmdir(d)

