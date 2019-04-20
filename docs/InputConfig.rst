Input Configuration
===================

Structure
---------

- The passed structure must be a list or a tuple
- Each entry in the structure must be a dictionary

Required Parameters
-------------------

- **table_name** (*str*): The name of the external table to be created
- **ip_addr** (*str*): The IP address of the FTP server
- **dir_ptrn** (*str*): A regular expression identifying the directories that contain the files of interest
- **file_ptrn** (*str*): A regular expression identifying the files of interest
- **file_ptrn_abbr** (*str*): A regular expression matching the shortest portion of *file_ptrn* that can still uniquely identify each file
- **columns** (*list*, *tuple*): An ordered list of the desired column names
- **schema** (*dict*):

    * Keys are the column names; amount and labels should match the length and values of *columns*
    * Values are datatypes recognizable by Pandas

        + datetime: *list*; index 0 is 'datetime', index 1 is the associated formatting string
        + Integer columns with NULL values: (*list*, *tuple*); index 0 is 'Int64', index 1 is the intended Pandas datatype (e.g. 'int32', 'int8')
        + All others: *str*; the name of the datatype

- **s3_bucket** (*str*): the name of the S3 bucket; directory paths in the bucket can be added (e.g. 'bucket/dir/subdir')


Optional Parameters
-------------------

- **mem_cap** (*int*, default=2 * 10 ** 9): The size in bytes that *df_main* can occupy in memory before writing. There will be some discrepancy between the value specified and the actual memory used.
- **row_skip** (*dict*)

    * Keys match the *file_ptrn_abbr* expression
    * Each value is a *dict* with two keys:

        + **rownums** ((*list*, *tuple*)): Each entry is an integer (row numbers to skip in the file identified by the parent key)
        + **rowrepls** ((*list*, *tuple*)): Each entry is a list of replacement values for each columns

- **partition** (*list*, *tuple*)

    * Index 0: A string with the name of the name of the partitions to be created
    * Index 1: The column with values to be used to partition the data
    * Index 2 (optional): If index 1 has type *datetime*, a string defining the format of the partition values (refer to date.strftime documentation)

Example Configuration
---------------------

.. code:: python

    [
        # Partitions, replacing rows, overriding default mem_cap
        {
            'table_name': 'ftp.tc_log',
            'mem_cap': 1 * 10 ** 9,
            'ip_addr': '1.2.3.4',
            'dir_ptrn': r'.*qtftp.*\\\d{6}$',
            'file_ptrn': r'file\.name\.\d{6}$',
            'file_ptrn_abbr': r'.{6}$',
            'row_skip': {
                '123456': {
                    'rownums': [999],
                    'rowrepls': [["01/01/2015", "23:30:00", "m2003", "51218", "Foo", 0.0, "T"]]
                }
            },
            'columns': ['col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7'],
            'schema': {
                'col1': ['datetime', '%m/%d/%Y'],
                'col2': ['datetime', '%H:%M:%S'],
                'col3': 'object',
                'col4': ['Int64', 'int32'],
                'col5': 'object',
                'col6': 'int8',
                'col7': 'object'
            },
            's3_bucket': 'bucket/dir/subdir',
            'partition': ['event_year', 'col1', '%Y']
        },
        # No partitions, no rows to skip, default mem_cap
        {
            'table_name': 'ftp.train_event',
            'ip_addr': '5.6.7.8',
            'dir_ptrn': r'.*train.*\\\d{6}$',
            'file_ptrn': r'event\.\d{6}$',
            'file_ptrn_abbr': r'.{6}$',
            'columns': ['date', 'time', 'id'],
            'schema': {
                'date': ['datetime', '%m/%d/%Y'],
                'time': ['datetime', '%H:%M:%S'],
                'id': 'object'
            },
            's3_bucket': 'bucket'
        }
    ]
