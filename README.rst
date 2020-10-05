datawelder
==========

Perform SQL-like `JOINs <https://en.wikipedia.org/wiki/Join_(SQL)>`_ on large file-like dataframes.

Do you have tons of larger-than-memory datasets lying around on your file system?
Do you dream of an easy way to join them together?
Do you want to achieve this without using a database?
If the answers to the above questions are "yes", then ``datawelder`` is for you!

Example
-------

::

    pip install datawelder
    python -m datawelder.partition s3://bucket/data/foo.csv.gz s3://bucket/parts/foo 1000
    python -m datawelder.partition s3://bucket/data/bar.csv.gz s3://bucket/parts/bar 1000
    python -m datawelder.join s3://bucket/data/foo_bar.csv.gz s3://bucket/parts/foo s3://bucket/parts/bar --writer csv
    
How does it work?
-----------------

First, ``datawelder`` `partitions <https://en.wikipedia.org/wiki/Partition_(database)>`_ each dataset using a partition key.
In the above example, we split each dataset into 1000 partititions using the default key (whatever is the first column), but you can override that.
Since the partitions now fit into memory, it is possible to join them one-by-one, and then concatenate the join results together to achieve the final joined dataset.
