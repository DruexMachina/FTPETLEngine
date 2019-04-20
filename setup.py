from setuptools import setup, find_packages

with open('README.rst', 'r') as infile:
    long_description = infile.read()

setup(
    name='FTPETLEngine',
    version='1.0',
    packages=find_packages(),
    author='Andrew Hile',
    author_email='ahile.int@transitchicago.com',
    install_requires=[
        "numpy",
        "pandas",
        "pyarrow",
        "s3fs"
    ],
    description='Performs the ETL process on files on an FTP server to '
                'Snappy-compressed parquet files in an S3 bucket',
    long_description=long_description,
    project_urls={
        'Source Code': ("https://console.aws.amazon.com/codesuite/"
                        "codecommit/repositories/data-analytics-pq-etl/"),
    }
)
