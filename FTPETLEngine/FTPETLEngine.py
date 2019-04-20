"""
Creates a data pipeline between an FTP server and S3 instance; files on the
server are concatenated and written to the S3 instance as Snappy-compressed
Parquet files

:class EngineConfig:    A wrapper for the input configuration
:class FTPETLEngine:    Performs the ETL process
"""
import ftplib
import gc
import os
from pathlib import Path
import re

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import s3fs

from .logger import logger
from .FTPWalk import FTPWalk


class EngineConfig:
    """
    Stores the keys and values of the input configuration dictionary in
    attributes for ease of use, then checks the contents of each attribute for
    type and structure validity. Refer to input config structure documentation
    for allowable parameters.

    :attribute mem_cap:             Threshold in bytes; a portion of
                                    main_df is written
                                    if the threshold is exceeded
    :method _validate:              Performs internal integrity checks on the
                                    input configuration parameters
    :method _validate_schema:       Performs internal integrity checks on the
                                    input schema parameters
    :method _validate_row_skip:     Performs internal integrity checks on the
                                    input row skip parameters
    :method _validate_partition:    Performs internal integrity checks on the
                                    input partition parameters
    :staticmethod key_check:        Ensures a parameter is present
    :staticmethod type_check:       Ensures a parameter is the proper type
    :staticmethod len_check:        Ensures an iterable parameter is the proper
                                    length
    :staticmethod re_check:         Ensures a parameter matches the given
                                    pattern
    :staticmethod iter_compare:     Ensures the contents of two iterables match
    """
    def __init__(self, config):
        """
        Initialize a logging instance, store the contents of the passed config
        in class attributes, then validate the attribute contents

        :param config:  Refer to input config structure documentation
        :type config:   dict
        """
        self.type_check(config, dict, 'config')
        for param, value in config.items():
            setattr(self, param, value)
        self.mem_cap = 2 * 10 ** 9
        self._validate()

    def _validate(self):
        """Check that the input config has the correct structure"""

        config_reqs = {
            'ip_addr': str,
            'dir_ptrn': str,
            'file_ptrn': str,
            'file_ptrn_abbr': str,
            'columns': (list, tuple),
            'schema': dict,
            's3_bucket': str
        }

        logger.info('Validating config parameters')
        for req_param, req_type in config_reqs.items():
            self.key_check(req_param, self.__dict__, 'config')
            self.type_check(getattr(self, req_param), req_type, f"{req_param}")

        logger.info('Validating schema parameters')
        self._validate_schema()
        self.iter_compare(
            'val', self.schema, self.columns, 'schema', 'columns')

        if 'row_skip' in self.__dict__:
            logger.info('Validating row_skip parameters')
            self._validate_row_skip()

        if 'partition' in self.__dict__:
            logger.info('Validating partition parameters')
            self._validate_partition()

        if self.mem_cap < 5 * 10 ** 8:
            logger.warning(
                'mem_cap is %i; a minimum size of 5 * 10 ** 8 is recommended',
                self.mem_cap)

    def _validate_schema(self):
        """Check that the input schema has the correct structure"""

        config_reqs = {
            'entry': (str, list, tuple),
            'length': 2
        }

        for col, type_def in self.schema.items():
            self.type_check(
                type_def, config_reqs['entry'], f"schema[\'{col}\']")
            if not isinstance(type_def, str):
                self.len_check(type_def, config_reqs['length'],
                               f"schema[\'{col}\']")
                for i, entry in enumerate(type_def):
                    self.type_check(entry, str, f"schema[\'{col}\'][{i}]")

    def _validate_row_skip(self):
        """Check that row skip parameters have the correct structure"""

        config_reqs = {
            'parent': dict,
            'entry': dict,
            'req_params': {
                'rownums': {
                    'parent': (list, tuple),
                    'entry': int
                },
                'rowrepls': {
                    'parent': (list, tuple),
                    'entry': (list, tuple)
                }
            }
        }

        self.type_check(self.row_skip, config_reqs['parent'], 'row_skip')
        for file, params in self.row_skip.items():
            self.re_check(
                file, self.file_ptrn_abbr, f"row_skip key \'{file}\'")
            for req_param in config_reqs['req_params']:
                self.key_check(req_param, params, f"row_skip[{file}]")
                self.type_check(
                    params[req_param],
                    config_reqs['req_params'][req_param]['parent'],
                    f"row_skip[{file}][\'{req_param}\']")
                for i, entry in enumerate(params[req_param]):
                    self.type_check(
                        entry,
                        config_reqs['req_params'][req_param]['entry'],
                        f"row_skip[{file}][\'{req_param}\'][{i}]")
            self.iter_compare('len', params['rownums'], params['rowrepls'],
                              f"row_skip[{file}][\'rownums\']",
                              f"row_skip[{file}][\'rowrepls\']")

    def _validate_partition(self):
        """Check that the partition parameters have the correct structure"""

        config_reqs = {
            'parent': (list, tuple),
            'entry': str,
            'length': [2, 3]
        }

        self.type_check(self.partition, config_reqs['parent'], 'partition')
        self.len_check(self.partition, config_reqs['length'], 'partition')
        for i, entry in enumerate(self.partition):
            self.type_check(entry, config_reqs['entry'], f"partition[{i}]")

    @staticmethod
    def key_check(key, obj, obj_name):
        """
        Customized error handling: simple dictionary lookup check

        :param key:         Name of a key to lookup in obj
        :type key:          str
        :param obj:         A dictionary
        :type obj:          dict
        :param obj_name:    Name of the offending object to print when raising
                            an error
        :type obj_name:     str
        :raises KeyError:   Raises error if the check fails
        """
        if key not in obj:
            raise KeyError(f"config: {key} not in {obj_name}")

    @staticmethod
    def type_check(obj, req_type, obj_name):
        """
        Customized error handling: simple type check

        :param obj:         Any object
        :param req_type:    Type to check against
        :type req_type:     type
        :param obj_name:    Name of the offending object to print when raising
                            an error
        :raises TypeError:  Raises error if the check fails
        """
        if not isinstance(obj, req_type):
            raise TypeError(f"config: {obj_name} must be a {repr(req_type)}")

    @staticmethod
    def len_check(obj, req_len, obj_name):
        """
        Customized error handling: simple equality check

        :param obj:         An iterable
        :param req_len:     Length(s) to check against
        :type req_len:      (int, tuple, list)
        :param obj_name:    Name of the offending object to print when raising
                            an error
        :raises ValueError: Raises error if the check fails
        """
        if isinstance(req_len, (list, tuple)):
            if len(obj) not in req_len:
                raise ValueError(
                    f"config: {obj_name} must have length {req_len}")
        else:
            if len(obj) != req_len:
                raise ValueError(
                    f"config: {obj_name} must have length {req_len}")

    @staticmethod
    def re_check(obj, ptrn, obj_name):
        """
        Customized error handling: regex match check

        :param obj:         A string
        :type obj:          str
        :param ptrn:        A regular expression
        :type ptrn:         str
        :param obj_name:    Name of the offending object to print when raising
                            an error
        :raises ValueError: Raises error if the check fails
        """
        if not re.match(ptrn, obj):
            raise ValueError(
                f"config: {obj_name} doesn't match pattern {ptrn}")

    @staticmethod
    def iter_compare(mode, obj1, obj2, name_obj1, name_obj2):
        """
        Customized error handling: compare either the length or contents of two
        iterables

        :param mode:        'len' or 'val' (determines what is compared)
        :type mode:         str
        :param obj1:        An iterable
        :param obj2:        An iterable
        :param name_obj1:   Name of the first offending object to print when
                            raising an error
        :type name_obj1:    str
        :param name_obj2:   Name of the second offending object to print when
                            raising an error
        :type name_obj2:    str
        :raises ValueError: Raises error if the check fails
        """
        if mode == 'len':
            if len(obj1) != len(obj2):
                raise ValueError(
                    f"config: {name_obj1}, {name_obj2} have unequal lengths")
        elif mode == 'val':
            col_match = set(obj1).intersection(obj2)
            for obj in [obj1, obj2]:
                if len(col_match) != len(obj):
                    raise ValueError(f"config: {name_obj1}, {name_obj2} do "
                                     "not have matching values")


class FTPETLEngine:
    """
    Houses methods needed to import files from an FTP server and write them
    using an input schema to an S3 bucket as Snappy-compressed Parquet files

    :attribute config:              An EngineConfig instance;
                                    converted from the input dictionary when
                                    initializing this class
    :attribute part_mode:           A string; value is determined by
                                    config.partition
    :attribute df_main:             A pandas.DataFrame instance; initially
                                    empty, accumulates the contents of the
                                    imported files
    :attribute mem:                 An integer; tracks the size of
                                    df_main in memory
    :attribute counter:             An integer; initially 0, increments if
                                    filenames
                                    will be duplicated when writing data
    :attribute rows:                An integer; tracks how many rows have been
                                    written
    :attribute s3fs_inst:           An s3fs.S3FileSystem instance
    :method etl:                    ETL workhorse method
    :method connect_s3:             Sets s3fs_inst
    :method import_file:            Appends file contents to df_main
    :method chunk_write:            Write a portion of df_main contents
                                    to a Snappy-compressed Parquet file
    :staticmethod gen_pa_schema:    Makes the input schema Pyarrow-compatible
    :staticmethod format_cols:      Applies a schema to a Pandas dataframe
    :staticmethod write_parquet:    Writes a Pandas dataframe to a
                                    Snappy-compressed Parquet file
    """
    def __init__(self, config):
        """
        Initialize class variables and a logging instance

        :param config:  Refer to input config structure documentation
        :type config:   dict
        """
        self.config = EngineConfig(config)
        if hasattr(self.config, 'partition'):
            self.part_mode = 'date' if self.config.partition[2] else 'other'
        else:
            self.part_mode = None
        self.df_main = pd.DataFrame()
        self.mem = 0
        self.counter = 0
        self.rows = 0
        self.s3fs_inst = None

    def etl(self):
        """
        Connect to the FTP server and import each file by appending its
        contents to df_main, then write a portion of the accumulated data
        to the S3 instance when either multiple partitions are present or the
        data exceeds the allowed memory capacity
        """
        if not self.s3fs_inst:
            raise ValueError("An s3fs instance has not been initialized, call "
                             "connect_s3 with a valid path to an AWS "
                             "credentials file to rectify")

        oversize_flag, head, tail = 0, '', ''
        pa_schema = self.gen_pa_schema(self.config.schema)

        logger.info('Scanning FTP server for matching files')
        with ftplib.FTP(self.config.ip_addr) as ftp:
            ftp.login()
            files = FTPWalk(ftp).get_files(self.config.dir_ptrn,
                                           self.config.file_ptrn)
        for file in files:
            logger.info(
                "Importing \'%s\'. Current memory usage: %i MB",
                file, round(self.mem / 1000000, 1))
            self.import_file(file)
            logger.info(
                "Imported \'%s\' successfully. Current memory usage: %i MB",
                file, round(self.mem / 1000000, 1))
            if self.part_mode:
                # Multiple partition values in partition column
                val_unique = self.df_main['partition'].unique()
                while len(val_unique) > 1:
                    logger.info(
                        "> 1 partition value present, writing partition %s",
                        val_unique[0])
                    # Siphon off a partition and write it
                    self.chunk_write(
                        'multipart', 'leaveRem', pa_schema,
                        part_val=val_unique[0])
                    val_unique = val_unique[1:]
            else:
                # Assign values to head and tail variables for use in filenames
                if not head:
                    head = re.search(self.config.file_ptrn_abbr, file)[0]
                tail = re.search(self.config.file_ptrn_abbr, file)[0]
            # Allowable memory capacity exceeded
            if self.mem > self.config.mem_cap:
                oversize_flag = 1
                logger.info("Oversize, writing chunk")
                if self.part_mode:
                    self.chunk_write(
                        'oversize', 'leaveRem', pa_schema,
                        part_val=val_unique[0])
                else:
                    self.chunk_write(
                        'oversize', 'leaveRem', pa_schema, head=head,
                        tail=tail)
                    head = tail
        if self.part_mode:
            self.chunk_write(
                'residue', 'writeAll', pa_schema, part_val=val_unique[0])
        elif oversize_flag:
            self.chunk_write(
                'residue', 'writeAll', pa_schema, head=head, tail=tail)
        else:
            self.chunk_write('residue', 'writeAll', pa_schema)
        gc.collect()
        logger.info("ETL complete, %i rows were exported", self.rows)

    def connect_s3(self, path='~/.aws/.credentials', use_ssl=False):
        """
        Create a connection to the S3 buckets available to an AWS account

        :param path:    Path to AWS credentials file
        :type path:     str
        """
        env_var = 'AWS_SHARED_CREDENTIALS_FILE'
        os.environ[env_var] = str(Path(path).expanduser())
        self.s3fs_inst = s3fs.S3FileSystem(use_ssl=use_ssl)

    def import_file(self, file):
        """
        Read the passed filename and append the contents to df_main

        :param file:   File on an FTP server to import
        :type file:    str
        """
        # Read file, skipping rows if needed
        rownums = []
        file_abbr = re.search(self.config.file_ptrn_abbr, file)[0]
        if hasattr(self.config, 'row_skip'):
            row_skip = self.config.row_skip
            if file_abbr in row_skip:
                rownums = row_skip[file_abbr]['rownums']
                rowrepls = row_skip[file_abbr]['rowrepls']
        df_read = pd.read_csv(f"ftp://{self.config.ip_addr}{file}",
                              header=None, skiprows=rownums)
        df_read.drop(df_read.columns[[-1]], axis=1, inplace=True)
        # If last row is mostly empty (error), remove it
        if df_read.tail(1).isnull().sum().sum() > 2:
            df_read.drop(df_read.tail(1).index, inplace=True)
        # Apply schema
        df_read.columns = self.config.columns
        self.format_cols(df_read, self.config.schema)
        # Insert replacement rows
        if hasattr(self.config, 'row_skip'):
            for i, rownum in enumerate(rownums):
                row = pd.DataFrame(np.array([rowrepls[i]]))
                row.columns = self.config.columns
                self.format_cols(row, self.config.schema)
                df_read = df_read[:rownum] \
                    .append([row, df_read[rownum:]]) \
                    .reset_index(drop=True)
        self.mem += df_read.memory_usage(deep=True).sum()
        # Create column based on partition name
        if self.part_mode:
            part_col = self.config.partition[1]
            if self.part_mode == 'date':
                part_format = self.config.partition[2]
                df_read['partition'] = df_read[part_col].map(
                    lambda x: x.strftime(part_format))
            elif self.part_mode == 'other':
                df_read['partition'] = df_read[part_col]
        self.df_main = self.df_main.append(
            df_read, ignore_index=True, sort=False)
        del df_read

    def chunk_write(self, _source, _mode, _pa_schema, **kwargs):
        """
        Extract a chunk of required size and write it to an S3 bucket as a
        Snappy-compressed Parquet file

        :param _source:     One of ['oversize', 'multipart', 'residue'], passed
                            from etl
        :type _source:      str
        :param _mode:       One of ['writeAll', 'leaveRem'], passed from etl
        :type _mode:        str
        :param _pa_schema:  Output from gen_pa_schema, passed from etl
        :type _pa_schema:   pyarrow.Schema
        :key head:          A str; first source file contained in df_main
        :key tail:          A str; last source file contained in df_main
        :key part_val:      A str; the partition name to write
        """
        # Check that _source and _mode have valid values
        if _source not in ['oversize', 'multipart', 'residue']:
            raise ValueError('Invalid source for method chunk_write')
        if _mode not in ['writeAll', 'leaveRem']:
            raise ValueError('Invalid mode for method chunk_write')

        s3_bucket = self.config.s3_bucket
        table_name = self.config.table_name

        while self.mem > self.config.mem_cap:
            # Construct filenames to export
            if self.part_mode:
                part_name = self.config.partition[0]
                filename = (
                    f"s3://{s3_bucket}/{table_name}/"
                    f"{part_name}={kwargs['part_val']}/"
                    f"{table_name}_{part_name}={kwargs['part_val']}_"
                    f"{str(self.counter).zfill(2)}.parquet.snappy")
            elif 'head' in kwargs and 'tail' in kwargs:
                filename = (
                    f"s3://{s3_bucket}/{table_name}/"
                    f"{table_name}_{kwargs['head']}_{kwargs['tail']}_"
                    f"{str(self.counter).zfill(2)}.parquet.snappy")
            else:
                filename = (
                    f"s3://{s3_bucket}/{table_name}.parquet.snappy")
            nrows = int(self.config.mem_cap / self.mem * self.df_main.shape[0])

            if _mode == 'leaveRem':
                # Siphon off the data that won't be written based on _source,
                # write the desired data, then reassign the remaining data to
                # self.df_main
                if _source == 'oversize':
                    df_rem = self.df_main[nrows:]
                elif _source == 'multipart':
                    df_rem = self.df_main[
                        self.df_main['partition'] != kwargs['part_val']]
                self.df_main.drop(df_rem.index, inplace=True)
                df_rem.reset_index(drop=True, inplace=True)
                if self.part_mode:
                    self.df_main.drop('partition', axis=1, inplace=True)
                self.write_parquet(
                    self.df_main, _pa_schema, filename, self.s3fs_inst)
                self.rows += self.df_main.shape[0]
                logger.info("Wrote %s", filename)
                self.mem = df_rem.memory_usage(deep=True).sum()
                self.df_main = df_rem
                if _source == 'oversize':
                    self.counter += 1
                if _source == 'multipart' or not self.part_mode:
                    self.counter = 0
            if _mode == 'writeAll':
                # Write the entire contents of self.df_main
                self.write_parquet(
                    self.df_main, _pa_schema, filename, self.s3fs_inst)
                self.rows += self.df_main.shape[0]
                logger.info("Wrote %s", filename)
                self.df_main = pd.DataFrame()
                self.counter = 0
            gc.collect()

    @staticmethod
    def gen_pa_schema(schema):
        """
        Convert a schema to one compatible with Pyarrow

        :param schema:  Refer to input config structure documentation
        :type schema:   dict

        :returns:       A pyarrow.Schema instance
        """
        pa_schema = []
        for key, val in schema.items():
            if val == 'object':
                pa_schema.append(pa.field(key, pa.string()))
            elif val == 'int8' or (val[0] == 'Int64' and val[1] == 'int8'):
                pa_schema.append(pa.field(key, pa.int8()))
            elif val in ['int16', 'int32'] or (val[0] == 'Int64' and
                                               val[1] in ['int16', 'int32']):
                pa_schema.append(pa.field(key, pa.int32()))
            elif val == 'int64' or (val[0] == 'Int64' and val[1] == 'int64'):
                pa_schema.append(pa.field(key, pa.int64()))
            elif val == 'float32':
                pa_schema.append(pa.field(key, pa.float32()))
            elif val == 'float64':
                pa_schema.append(pa.field(key, pa.float64()))
            elif val[0] == 'datetime':
                pa_schema.append(pa.field(key, pa.timestamp('ms')))
            else:
                raise ValueError('Undefined type')
        return pa.schema(pa_schema)

    @staticmethod
    def format_cols(df, schema):
        """
        Format the columns of a Pandas dataframe in-place according to the
        passed schema (as a dict, not a pyarrow.Schema instance)

        :param df:      Pandas dataframe to process
        :type df:       pandas.DataFrame
        :param schema:  Refer to input config structure documentation
        :type schema:   dict
        """
        for col, col_def in schema.items():
            if col_def in ['int8', 'int16', 'int32', 'int64', 'float16',
                           'float32', 'float64', 'category']:
                try:
                    df[col] = df[col].astype(col_def)
                except ValueError:
                    df[col] = df[col].astype(float).astype(col_def)
            elif col_def == 'object':
                pass
            elif col_def[0] == 'Int64':
                df[col] = df[col].astype('float64')
            elif col_def[0] == 'datetime':
                df[col] = pd.to_datetime(df[col], format=col_def[1])
                df[col] = df[col].astype('datetime64[s]')
            else:
                raise ValueError("Undefined type", col_def)

    @staticmethod
    def write_parquet(df, pa_schema, filename, s3fs_inst):
        """
        Use Pyarrow and the passed schema to write a Pandas dataframe to a
        Snappy-compressed Parquet file

        :param df:          Pandas dataframe to write to a Parquet file
        :type df:           pandas.DataFrame
        :param filename:    File path to write to in an S3 bucket
        :type filename:     str
        """
        pq.write_table(
            pa.Table.from_pandas(
                df, schema=pa_schema, preserve_index=False),
            filename, filesystem=s3fs_inst)
