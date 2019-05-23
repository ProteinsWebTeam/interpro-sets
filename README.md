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

Defined with `-t`. Default: 1.

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
python run.py cdd [--dir TEMPORARY_DIRECTORY] [-t NUM_THREADS] [--sequences CDDMASTER] [--links FAMILY_SUPERFAMILY_LINKS]
```

`--sequences`: FASTA file of representative sequences for each domain. Default: downloaded from CDD FTP.

`--links`: file containing CDD domain and superfamily information. Default: downloaded from CDD FTP.

### PANTHER superfamilies

```bash
python run.py panther --books BOOKS_DIRECTORY [--dir TEMPORARY_DIRECTORY] [-t NUM_THREADS]
```

`--books`: directory of PANTHER "books", each representing a protein family (expects a `hmmer.hmm` file for each book).

### Pfam clans

```bash
python run.py pfam [--dir TEMPORARY_DIRECTORY] [-t NUM_THREADS] [--hmm PFAM-A] [--clans PFAM_CLANS]
```

`--hmm`: file containing the Pfam-A HMMs. Default: downloaded from Pfam FTP.

`--clans`: tab-separated file containing Pfam-A family and clan information. Default: downloaded from Pfam FTP.

### PIRSF superfamilies

```bash
python run.py pirsf --hmm SF_HMM_ALL [--dir TEMPORARY_DIRECTORY] [-t NUM_THREADS] [--info PIRSFINFO]
```

`--hmm`: file containing the PIRSF HMMs.

`--info`: file containing PIRSF family and superfamily information. Default: downloaded from PIRSF FTP.

### Web application

Using Flask's built-in server:

```bash
export FLASK_APP=interprosets/server.py
export INTERPRO_URI="user/password@[host:port/]service"
flask run
```

Using Gunicorn:

```bash
export INTERPRO_URI="user/password@[host:port/]service"
gunicorn interprosets.server:app
```

## Resource usage

| database     | families | threads     | memory usage | disk usage | Time     |
|--------------|---------:|------------:|-------------:|-----------:|---------:|
| cdd 3.17     |    14877 |          16 |         1 GB |     1.5 GB |     12 h |
| panther 14.1 |    90742 |           8 |        32 GB |     105 GB |    100 h |
| pfam 32.0    |    17929 |           8 |         3 GB |       5 GB |      1 h |
| pirsf 3.02   |     3283 |           8 |         2 GB |       2 GB |     15 m |

