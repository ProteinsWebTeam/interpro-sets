# interpro-sets

This repo contains the source-code to perform profile-profile alignments of Pfam/PIRSF/PANTHER/CDD signatures, and the source-code and static files for the Flask application (prototype) to explore data.

## Installation

None required. (」・ω・)」

### Dependencies

* Python 3.3+ with `cx_Oracle` and `Flask`.
* [HMMER3](http://hmmer.org/) for PANTHER, Pfam, and PIRSF.
* [COMPASS](http://prodata.swmed.edu/download/pub/compass/) for CDD.

## Usage

Common options:

**Oracle connection string**

The `INTERPRO_URI` environment variable is used to pass the Oracle connection string to the script. It can be set with the following command:

```bash
export INTERPRO_URI="user/password@host:port/service"
```

`host` and `port` may be omitted, depending on your Oracle TNS configuration.

**Temporary directory**

`--dir` specifies the directory for temporary files. It is created if it does not exist, and temporary files are deleted on completion (the directory itself is not deleted). Default: depends on your platform; probably `/tmp/` on Unix-based systems.

**Number of threads**

Defined with `-p`. Default: 1.

**Paths to binaries**

If HMMER (`hmmconvert`, `hmmpress`, `hmmemit`, `hmmscan`) or COMPASS (`mk_compass_db`, `compass_vs_db`) binaries are not in your `PATH` (e.g. installed on a NFS mounts), you can use the following command to add them:

```bash
export PATH=/path/to/directory/:$PATH
```

### Database tables

Drop the `METHOD_SET` and `METHOD_SCAN` tables if they exist in the `INTERPRO` Oracle schema, then create them.

```bash
python run.py init
```

### CDD superfamilies

```bash
python run.py cdd [--dir TEMPORARY_DIRECTORY] [-p NUM_PROCESSES] [--sequences CDDMASTER] [--links FAMILY_SUPERFAMILY_LINKS]
```

`--sequences`: FASTA file of representative sequences for each domain. Default: downloaded from CDD FTP.

`--links`: file containing CDD domain and superfamily information. Default: downloaded from CDD FTP.

### PANTHER superfamilies

```bash
python run.py panther --books BOOKS_DIRECTORY [--dir TEMPORARY_DIRECTORY] [-p NUM_PROCESSES]
```

`--books`: directory of PANTHER "books", each representing a protein family (expects a `hmmer.hmm` file for each book).

### Pfam clans

```bash
python run.py pfam [--dir TEMPORARY_DIRECTORY] [-p NUM_PROCESSES] [--hmm PFAM-A] [--clans PFAM_CLANS]
```

`--hmm`: file containing the Pfam-A HMMs. Default: downloaded from Pfam FTP.

`--clans`: tab-separated file containing Pfam-A family and clan information. Default: downloaded from Pfam FTP.

### PIRSF superfamilies

```bash
python run.py pirsf --hmm SF_HMM_ALL [--dir TEMPORARY_DIRECTORY] [-p NUM_PROCESSES] [--info PIRSFINFO]
```

`--hmm`: file containing the PIRSF HMMs.

`--info`: file containing PIRSF family and superfamily information. Default: downloaded from PIRSF FTP.

### Web application

Using Flask's built-in server:

```bash
export FLASK_APP=interprosets/server.py
export INTERPRO_SETS_URI="user/password@[host:port/]service"
flask run
```

Using Gunicorn:

```bash
export INTERPRO_SETS_URI="user/password@[host:port/]service"
gunicorn interprosets.server:app
```

## Resource usage

| command   | families | processes   | memory usage | disk usage | Time     |
|-----------|---------:|------------:|-------------:|-----------:|---------:|
| pirsf     |     3283 |           8 |         8 GB |     1.3 GB |     15 m |
| pfam      |    17929 |           8 |        16 GB |       3 GB |      1 h |
| cdd       |    12774 |           8 |         2 GB |       1 GB |     20 h |
| panther   |    90742 |          16 |        27 GB |      40 GB |     40 h |
