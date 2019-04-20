FTPETLEngine package
====================

Connects to an FTP server and identifies files to ETL based on input regular expressions. These files are concatenated and exported to an S3 bucket as snappy-compressed parquet files using parameters in a passed configuration. Partition creation functionality is included. Export operations will create a fairly uniform file size distribution.

Files
-----

- *FTPETLEngine.py*: Contains the functions that perform the ETL process
- *FTPWalk.py*: Houses the methods used to scan an FTP server
- *logger.py*: Sets up the package logging instance

Installation
------------

.. code:: bash

    cd FTPETLEngine
    python setup.py install

Usage
-----

.. code:: python

    etl_params = [
        {
            # Configuration structure
        }
    ]

    for config in etl_params:
        engine = FTPETLEngine(config)
        engine.connect_s3()
        engine.etl()
