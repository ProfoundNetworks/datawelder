datawelder
==========

Perform SQL-like `JOINs <https://en.wikipedia.org/wiki/Join_(SQL)>`_ on large file-like dataframes.

- Do you have tons of larger-than-memory datasets lying around on your file system?
- Do you dream of an easy way to join them together?
- Do you want to achieve this without using a database?

If the answers to the above questions are "yes", then ``datawelder`` is for you!

Example
-------

First, examine our toy dataset.
It contains country names and currencies in two separate tables.

::

    $ head -n 5 sampledata/names.csv
    iso3|name
    AND|Principality of Andorra
    ARE|United Arab Emirates
    AFG|Islamic Republic of Afghanistan
    ATG|Antigua and Barbuda
    $ head -n 5 sampledata/currencies.csv
    iso3|currency
    AND|Euro
    ARE|Dirham
    AFG|Afghani
    ATG|Dollar

We can join these two dataframes as follows:

::

    $ pip install datawelder
    $ python -m datawelder.partition sampledata/names.csv partitions/names 5
    $ python -m datawelder.partition sampledata/currencies.csv partitions/currencies 5
    $ python -m datawelder.join out.csv partitions/names partitions/currencies --format csv
    $ grep AND out.csv
    AND|Principality of Andorra|Euro

Tweaking
--------

You can specify the partition key explicitly:

::

    $ python -m datawelder.partition sampledata/names.csv partitions/names 5 --keyindex 0
    $ python -m datawelder.partition sampledata/names.csv partitions/names 5 --keyname iso3

You can specify any format parameters (e.g. CSV delimiter) explicitly:

::

    $ python -m datawelder.partition sampledata/names.csv partitions/names 5 --formatparams delimiter='|' quotechar=''

Similarly, for output:

::

    $ python -m datawelder.join out.csv partitions/names partitions/currencies --format csv --fmtparams delimiter=,
    $ grep AND out.csv
    AND,Principality of Andorra,Euro

You can also select a subset of fields to keep (similar to SQL SELECT):

::

    $ python -m datawelder.join out.csv partitions/names partitions/currencies --format csv --select name currency
    $ grep -i andorra out.csv
    Principality of Andorra,Euro
    
How does it work?
-----------------

First, ``datawelder`` `partitions <https://en.wikipedia.org/wiki/Partition_(database)>`_ each dataset using a partition key.
We used 5 partitions because the datasets are tiny, but you can specify an arbitrary partition size when working with real data.

In this case, it automatically identified the format of the file as CSV.
You can give it a helping hand by specifying the format and relevant parameters (e.g. field separator, quoting, etc) manually.

We did not specify a partition key to use in the above example, so ``datawelder`` picked a default for us (you can override this).
In the above example, we split each dataset into 10 partititions using the default key (whatever is the first column), but you can override that.

Features
--------

- Parallelization across multiple cores via subprocess/multiprocessing
- Access to cloud storage for reading and writing e.g. S3 via `smart_open <https://github.com/RaRe-Technologies/smart_open>`_.  You do not have to store anything locally.
- Read/write various file formats (CSV, JSON, pickle) out of the box
- Flexible API for dealing with file format edge cases
