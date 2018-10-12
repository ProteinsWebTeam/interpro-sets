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
    return g.database


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

    return jsonify(databases), 200


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

    return jsonify(sets), 200


@app.route('/api/set/<accession>/')
def api_set_members(accession):
    cur = get_db().cursor()
    cur.execute(
        """
        SELECT sm.method_ac,
          sm.name,
          COUNT(*),
          CAST(SUM(CASE WHEN (sm2.set_ac IS NULL) THEN 1 ELSE 0 END) AS UNSIGNED),
          CAST(SUM(CASE WHEN (sm2.set_ac IS NOT NULL AND sm2.set_ac != sm.set_ac) THEN 1 ELSE 0 END) AS UNSIGNED)
        FROM set_method sm
          INNER JOIN method_target mt ON sm.method_ac = mt.method_ac
          LEFT OUTER JOIN set_method sm2 on mt.target_ac = sm2.method_ac
        WHERE sm.set_ac = %s
        GROUP BY sm.method_ac
        ORDER BY sm.method_ac
        """,
        (accession,)
    )

    cols = ('accession', 'name', 'targets', 'targets_no_set', 'targets_other_set')
    members = [dict(zip(cols, row)) for row in cur]
    cur.close()
    return jsonify(members), 200 if members else 404


@app.route('/api/entry/<accession>/targets/')
def api_entry_targets(accession):
    cur = get_db().cursor()
    cur.execute(
        """
        SELECT name, set_ac, sequence
        FROM set_method
        WHERE method_ac = %s
        """,
        (accession,)
    )
    row = cur.fetchone()

    name = set_ac = sequence = None
    targets = []
    if row:
        name, set_ac, sequence = row

        cur.execute(
            """
            SELECT sm.method_ac, sm.name, sm.set_ac, mt.evalue, mt.domains
            FROM method_target mt
            INNER JOIN set_method sm on mt.target_ac = sm.method_ac
            WHERE mt.method_ac = %s
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
                    'domains': json.loads(row[4])
                })

    cur.close()

    return jsonify({
        'accession': accession,
        'name': name,
        'sequence': sequence,
        'set': set_ac,
        'targets': sorted(
            targets,
            key=lambda x: (0 if x['set'] != set_ac else 1, x['evalue'])
        )
    }), 200 if name else 404


@app.route('/api/set/<accession>/relationships/')
def api_relationships(accession):
    cur = get_db().cursor()
    cur.execute(
        """
        SELECT sm.method_ac, sm.name, sm2.method_ac, sm2.name, mt.evalue
        FROM set_method sm
        INNER JOIN method_target mt ON sm.method_ac = mt.method_ac
        INNER JOIN set_method sm2 ON mt.target_ac = sm2.method_ac
        WHERE sm.set_ac = %(acc)s AND sm2.set_ac = %(acc)s
        """,
        dict(acc=accession)
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

    return jsonify({
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
    sim_type = request.args.get('type', 'evalue')
    if sim_type not in ('evalue', 'collocation', 'overlap'):
        return jsonify({}), 400

    cur = get_db().cursor()
    cur.execute(
        """
        SELECT method_ac, name
        FROM set_method
        WHERE set_ac = %s
        ORDER BY method_ac
        """,
        (accession,)
    )

    methods = {row[0]: {'accession': row[0], 'name': row[1]} for row in cur}
    accessions = list(methods.keys())
    methods = sorted(methods.values(), key=lambda x: x['accession'])

    if sim_type == 'evalue':
        m = [[None] * len(accessions) for _ in accessions]
    else:
        m = []
        for i in range(len(accessions)):
            m.append([0] * len(accessions))
            m[i][i] = 1

    if accessions:
        if sim_type == 'evalue':
            cur.execute(
                """
                SELECT method_ac, target_ac, evalue
                FROM method_target
                WHERE method_ac IN ({0}) AND target_ac IN ({0})
                """.format(','.join(['%s' for _ in accessions])),
                (*accessions, *accessions)
            )

            for method_ac, target_ac, evalue in cur:
                i = accessions.index(method_ac)
                j = accessions.index(target_ac)

                if m[i][j] is None or evalue < m[i][j]:
                    m[i][j] = m[j][i] = evalue
        else:
            cur.execute(
                """
                SELECT method_ac1, method_ac2, collocation, overlap
                FROM method_similarity
                WHERE method_ac1 IN ({0})
                AND method_ac2 IN ({0})
                """.format(','.join(['%s' for _ in accessions])),
                (*accessions, *accessions)
            )

            x = 2 if sim_type == 'collocation' else 3
            for row in cur:
                i = accessions.index(row[0])
                j = accessions.index(row[1])
                m[i][j] = m[j][i] = row[x]

    cur.close()

    return jsonify({
        'accession': accession,
        'methods': methods,
        'data': m
    })


@app.route('/api/set/<accession>/entries/')
def api_set_entries(accession):
    cur = get_db().cursor()
    cur.execute(
        """
        SELECT method_ac, name, targets, targets_long
        FROM method_hmmscan
        WHERE parent = %s
        ORDER BY method_ac
        """,
        (accession, )
    )

    entries = []
    hits = {}
    for row in cur:
        acc = row[0]
        targets = json.loads(row[2] if row[2] else row[3])
        #targets = [e['accession'] for e in targets if e['accession'] != acc]
        entries.append({
            'accession': row[0],
            'name': row[1],
            'num_hits': len(targets)
        })

    cur.close()

    return jsonify({
        'accession': accession,
        'entries': entries
    })


@app.route('/api/hits/<accession>/')
def api_entry_hits(accession):
    cur = get_db().cursor()
    cur.execute(
        """
        SELECT name, parent, sequence, sequence_long, targets, targets_long
        FROM method_hmmscan
        WHERE method_ac = %s
        """,
        (accession, )
    )

    row = cur.fetchone()
    if row:
        name = row[0]
        parent = row[1]
        sequence = row[2] if row[2] else row[3]
        targets = json.loads(row[4] if row[4] else row[5])
        accessions = [t['accession'] for t in targets]

        cur.execute(
            """
            SELECT method_ac, name, parent
            FROM method_hmmscan
            WHERE method_ac IN ({})
            """.format(','.join(['%s' for _ in accessions])),
            accessions
        )

        entries = {}
        for row in cur:
            entries[row[0]] = {
                'name': row[1],
                'parent': row[2]
            }

        for t in targets:
            try:
                t.update(entries[t['accession']])  # should *never* throws a key error
            except KeyError:
                print(t)

        cur.close()
        return jsonify({
            'accession': accession,
            'name': name,
            'parent': parent,
            'sequence': sequence,
            'hits': targets
        })
    else:
        cur.close()
        return jsonify({}), 404


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/set/<accession>/')
def page_set(accession):
    return render_template('set.html')
