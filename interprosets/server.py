#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import cx_Oracle
from flask import json
from flask import Flask, g, render_template, request


try:
    URI = os.environ["INTERPRO_SETS_URI"]
except KeyError:
    raise ValueError("'INTERPRO_SETS_URI' not set")

app = Flask(__name__)


def get_db():
    if not hasattr(g, "con"):
        g.con = cx_Oracle.connect(URI)
    return g.con


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, "con"):
        g.con.close()


@app.route('/api/databases/')
def api_databases():
    cur = get_db().cursor()
    cur.execute(
        """
        SELECT DISTINCT DBNAME, DBSHORT
        FROM INTERPRO.CV_DATABASE
        INNER JOIN INTERPRO.METHOD_SET USING (DBCODE)
        ORDER BY DBNAME
        """
    )

    databases = [dict(zip(("name", "id"), row)) for row in cur]
    cur.close()

    return json.jsonify(databases), 200


@app.route('/api/database/<dbshort>/')
def api_database(dbshort):
    cur = get_db().cursor()
    cur.execute(
        """
        SELECT SET_AC, COUNT(*)
        FROM INTERPRO.CV_DATABASE
        INNER JOIN INTERPRO.METHOD_SET USING (DBCODE)
        WHERE DBSHORT = :1
        AND SET_AC IS NOT NULL
        GROUP BY SET_AC
        ORDER BY SET_AC
        """,
        (dbshort,)
    )

    sets = [dict(zip(("accession", "count"), row)) for row in cur]
    cur.close()

    return json.jsonify(sets), 200


@app.route('/api/set/<accession>/')
def api_set_members(accession):
    cur = get_db().cursor()
    cur.execute(
        """
        SELECT 
          Q.METHOD_AC,
          MIN(M.NAME),
          COUNT(*),
          CAST(
            SUM(
              CASE WHEN (
                T.SET_AC IS NULL
              )
              THEN 1 
              ELSE 0 
              END
            ) 
            AS NUMBER
          ),
          CAST(
            SUM(
              CASE WHEN (
                T.SET_AC IS NOT NULL AND T.SET_AC != Q.SET_AC
              ) 
              THEN 1 
              ELSE 0 
              END
            ) 
            AS NUMBER
          )
        FROM INTERPRO.METHOD_SET Q
        INNER JOIN INTERPRO.METHOD_SCAN SC 
          ON Q.METHOD_AC = SC.QUERY_AC
        LEFT OUTER JOIN INTERPRO.METHOD_SET T 
          ON SC.TARGET_AC = T.METHOD_AC 
        LEFT OUTER JOIN INTERPRO.METHOD M 
          ON Q.METHOD_AC = M.METHOD_AC
        WHERE Q.SET_AC = :1
        GROUP BY Q.METHOD_AC
        ORDER BY Q.METHOD_AC
        """,
        (accession,)
    )

    cols = (
        "accession",
        "name",
        "targets",
        "targets_without_set",
        "targets_other_set"
    )
    members = [dict(zip(cols, row)) for row in cur]
    cur.close()
    return json.jsonify(members), 200 if members else 404


@app.route('/api/entry/<accession>/targets/')
def api_entry_targets(accession):
    cur = get_db().cursor()
    cur.execute(
        """
        SELECT NAME, SET_AC, SEQUENCE
        FROM INTERPRO.METHOD_SET
        LEFT OUTER JOIN INTERPRO.METHOD USING (METHOD_AC)
        WHERE METHOD_AC = :1
        """,
        (accession,)
    )
    row = cur.fetchone()

    name = set_ac = sequence = None
    targets = []
    if row:
        name, set_ac, sequence = row
        sequence = sequence.read()

        cur.execute(
            """
            SELECT SC.TARGET_AC, M.NAME, SE.SET_AC, SC.EVALUE, SC.DOMAINS
            FROM INTERPRO.METHOD_SCAN SC
            INNER JOIN INTERPRO.METHOD_SET SE 
              ON SC.TARGET_AC = SE.METHOD_AC
            LEFT OUTER JOIN INTERPRO.METHOD M ON SC.TARGET_AC = M.METHOD_AC
            WHERE SC.QUERY_AC = :1
            """,
            (accession,)
        )

        for row in cur:
            if row[0] != set_ac:
                targets.append({
                    'accession': row[0],
                    'name': row[1],
                    'set': row[2],
                    'evalue': row[3],
                    'domains': json.loads(row[4].read())
                })

    cur.close()

    return json.jsonify({
        'accession': accession,
        'name': name,
        'sequence': sequence,
        'set': set_ac,
        'targets': sorted(
            targets,
            key=lambda x: (0 if x['set'] != set_ac else 1, x['evalue'])
        )
    }), 200 if sequence else 404


@app.route('/api/set/<accession>/relationships/')
def api_relationships(accession):
    cur = get_db().cursor()
    cur.execute(
        """
        SELECT 
          SC.QUERY_AC, M1.NAME, SC.TARGET_AC, M2.NAME, SC.EVALUE
        FROM INTERPRO.METHOD_SCAN SC
        INNER JOIN INTERPRO.METHOD_SET Q
          ON SC.QUERY_AC = Q.METHOD_AC
        INNER JOIN INTERPRO.METHOD_SET T
          ON SC.TARGET_AC = T.METHOD_AC
        LEFT OUTER JOIN INTERPRO.METHOD M1
          ON Q.METHOD_AC = M1.METHOD_AC
        LEFT OUTER JOIN INTERPRO.METHOD M2
          ON T.METHOD_AC = M2.METHOD_AC
        WHERE Q.SET_AC = :1 AND T.SET_AC = :1
        """,
        (accession,)
    )

    nodes = {}
    edges = {}

    for row in cur:
        method_ac = row[0]
        method_name = row[1]
        target_ac = row[2]
        target_name = row[3]
        evalue = row[4]

        if method_ac not in nodes:
            nodes[method_ac] = {
                'accession': method_ac,
                'name': method_name
            }

        if target_ac not in nodes:
            nodes[target_ac] = {
                'accession': target_ac,
                'name': target_name
            }

        if method_ac > target_ac:
            method_ac, target_ac = target_ac, method_ac

        if method_ac not in edges:
            edges[method_ac] = {target_ac: evalue}
        elif target_ac not in edges[method_ac] or evalue < edges[method_ac][target_ac]:
            edges[method_ac][target_ac] = evalue

    cur.close()

    return json.jsonify({
        'accession': accession,
        'data': {
            'nodes': list(nodes.values()),
            'links': [
                {
                    'source': acc1,
                    'target': acc2,
                    'value': edges[acc1][acc2]
                }
                for acc1 in edges
                for acc2 in edges[acc1]
            ]
        }
    }), 200 if nodes else 404


@app.route('/api/set/<accession>/similarity/')
def api_set_similarity(accession):
    cur = get_db().cursor()
    cur.execute(
        """
        SELECT METHOD_AC, NAME
        FROM INTERPRO.METHOD_SET MS
        LEFT OUTER JOIN INTERPRO.METHOD USING (METHOD_AC)
        WHERE SET_AC = :1
        ORDER BY METHOD_AC
        """,
        (accession,)
    )

    methods = {row[0]: {'accession': row[0], 'name': row[1]} for row in cur}
    accessions = list(methods.keys())
    methods = sorted(methods.values(), key=lambda x: x['accession'])

    m = [[None] * len(accessions) for _ in accessions]

    if accessions:
        cur.execute(
            """
            SELECT 
              SC.QUERY_AC, M1.NAME, SC.TARGET_AC, M2.NAME, SC.EVALUE
            FROM INTERPRO.METHOD_SCAN SC
            INNER JOIN INTERPRO.METHOD_SET Q
              ON SC.QUERY_AC = Q.METHOD_AC
            INNER JOIN INTERPRO.METHOD_SET T
              ON SC.TARGET_AC = T.METHOD_AC
            LEFT OUTER JOIN INTERPRO.METHOD M1
              ON Q.METHOD_AC = M1.METHOD_AC
            LEFT OUTER JOIN INTERPRO.METHOD M2
              ON T.METHOD_AC = M2.METHOD_AC
            WHERE Q.SET_AC = :1 AND T.SET_AC = :1
            """,
            (accession,)
        )

        for query_ac, _, target_ac, _, evalue in cur:
            i = accessions.index(query_ac)
            j = accessions.index(target_ac)
            if m[i][j] is None or evalue < m[i][j]:
                m[i][j] = m[j][i] = evalue

    cur.close()

    return json.jsonify({
        'accession': accession,
        'methods': methods,
        'data': m
    })


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/set/<accession>/')
def page_set(accession):
    return render_template('set.html')
