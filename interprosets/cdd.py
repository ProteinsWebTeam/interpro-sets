#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
import re
from tempfile import mkstemp

import cx_Oracle

from . import utils

SEQUENCES = "ftp://ftp.ncbi.nlm.nih.gov/pub/mmdb/cdd/cddmasters.fa.gz"
LINKS = "ftp://ftp.ncbi.nlm.nih.gov/pub/mmdb/cdd/family_superfamily_links"
DBCODE = "J"


def parse_superfamilies(filepath):
    fam2set = {}
    p1 = re.compile(r"cd\d+")
    p2 = re.compile(r"cl\d+")
    for line in utils.iterlines(filepath):
        cols = line.rstrip().split("\t")
        fam_acc = cols[0]
        set_acc = cols[2]

        if p1.match(fam_acc) and p2.match(set_acc):
            fam2set[fam_acc] = set_acc

    return fam2set


def run(uri, cdd_masters=None, links=None, processes=1, tmpdir=None):
    if cdd_masters is None:
        fd, cdd_masters = mkstemp(
            suffix=os.path.basename(SEQUENCES), dir=tmpdir
        )
        os.close(fd)

        utils.download(SEQUENCES, cdd_masters)

    if links is None:
        fd, links = mkstemp(suffix=os.path.basename(LINKS), dir=tmpdir)
        os.close(fd)
        utils.download(LINKS, links)

    utils.logger("extract sequences")
    p = re.compile(r">(gnl\|CDD\|\d+)\s+(cd\d+),")
    buffer = ""
    acc = None
    id2acc = {}
    entries = {}
    for line in utils.iterlines(cdd_masters):
        if line[0] == ">":
            if buffer and acc:
                entries[acc] = buffer

            m = p.match(line)
            if m:
                acc = m.group(2)
                id2acc[m.group(1)] = acc
            else:
                acc = None

            buffer = ""

        buffer += line

    if buffer and acc:
        entries[acc] = buffer

    fd, files_list = mkstemp(dir=tmpdir)
    os.close(fd)

    dirs = []
    with open(files_list, "wt") as fh:
        for acc in entries:
            _dir = os.path.join(tmpdir, acc[:5])
            try:
                os.mkdir(_dir)
            except FileExistsError:
                pass
            else:
                dirs.append(_dir)

            fa_file = os.path.join(_dir, acc + ".fa")
            with open(fa_file, "wt") as fh2:
                fh2.write(entries[acc])

            fh.write("{}\n".format(fa_file))
            entries[acc] = fa_file

    utils.logger("parse superfamilies")
    fam2set = parse_superfamilies(links)

    utils.logger("make profile database")
    fd, profile_db = mkstemp(dir=tmpdir)
    os.close(fd)
    utils.mk_compass_db(files_list, profile_db)

    jobs = [(acc, entries[acc], profile_db) for acc in entries]
    con = cx_Oracle.connect(uri)
    utils.prepare_tables(con, DBCODE)
    cur1 = con.cursor()
    cur2 = con.cursor()
    cur2.setinputsizes(evalue=cx_Oracle.NATIVE_FLOAT)
    cnt = 0
    data1 = []
    data2 = []
    utils.logger("run compass: {:>10} / {}".format(cnt, len(jobs)))
    for acc, fa_file, out_file in utils.batch_compass(jobs, processes):
        sequence, _ = utils.read_fasta(fa_file)
        targets = utils.parse_compass_results(out_file)

        data1.append((
            acc,
            DBCODE,
            fam2set.get(acc),
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
            t_id = t["id"]
            t_acc = id2acc[t_id]
            if acc == t_acc:
                continue

            data2.append({
                "query_ac": acc,
                "target_ac": t_acc,
                "evalue": t["evalue"],
                "evaluestr": t["evaluestr"],
                "domains": json.dumps([
                    {
                        "query": t["sequences"]["query"],
                        "target": t["sequences"]["target"],
                        "ievalue": None,
                        "start": t["start"],
                        "end": t["end"]
                    }
                ])
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
            utils.logger("run compass: {:>10} / {}".format(cnt, len(jobs)))

    utils.logger("run compass: {:>10} / {}".format(cnt, len(jobs)))

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
