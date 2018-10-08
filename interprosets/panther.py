#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os


def find_hmm_files(path):
    entries = {}
    for root, dirs, files in os.walk(path):
        for f in files:
            if f == "hmmer.hmm":
                filepath = os.path.join(root, f)

                head, tail = os.path.split(os.path.dirname(filepath))
                if tail.startswith("PTHR"):
                    acc = tail
                else:
                    acc = os.path.split(head)[1] + ':' + tail

                entries[acc] = filepath

    return entries


def run(books_path):
    pass