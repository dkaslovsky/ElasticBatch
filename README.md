# ElasticBatch

Elasticsearch buffer for collecting and batch inserting Python data and pandas DataFrames

[![Build Status](https://travis-ci.com/dkaslovsky/ElasticBatch.svg?branch=master)](https://travis-ci.com/dkaslovsky/ElasticBatch)
[![Coverage Status](https://coveralls.io/repos/github/dkaslovsky/ElasticBatch/badge.svg?branch=master)](https://coveralls.io/github/dkaslovsky/ElasticBatch?branch=master)
[![Python 3.6](https://img.shields.io/badge/python-3.6-blue.svg)](https://www.python.org/downloads/release/python-360/)
[![Python 3.7](https://img.shields.io/badge/python-3.7-blue.svg)](https://www.python.org/downloads/release/python-370/)
[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/)


### Updates
2019-12-14: Work in progress nearing completion; full description to come

### Overview
An efficient pattern when processing data bound for [Elasticsearch](https://www.elastic.co/products/elasticsearch) is to collect data records ("documents") in a buffer to be bulk-inserted into Elasticsearch in a single batch.  `ElasticBatch` provides this functionality to ease the overhead and reduce the code involved in inserting large batches or streams of data into Elasticsearch.  In particular, `ElasticBatch` makes it easy to efficiently insert batches of data in the form of Python dictionaries or [pandas](https://pandas.pydata.org/) [DataFrames](https://pandas.pydata.org/pandas-docs/stable/getting_started/dsintro.html#dataframe) into Elasticsearch.

### Features
`ElasticBatch` implements the following features (see [Usage](#usage) for examples and more details):
- Work with documents as lists of dicts or as rows of pandas DataFrames
- Add documents to a buffer that will automatically flush (insert its contents to Elasticsearch) when it is full
- Handle all interaction with and configuration of the underlying Elasticsearch client while remaining configurable by the user if so desired
- Track the elapsed time a document has been in the buffer, allowing a user to flush the buffer at a desired time interval even when it is not full
- Work within a context manager that will automatically flush before exiting, alleviating the need for extra code to ensure all documents are written to the database
- Optionally dump the buffer contents (documents) to a file before exiting due to an uncaught exception
- __Future__: Programatically add Elasticsearch metadata to each document
- __Future__: Automatically ack messages immediately after successful insertion into Elasticsearch when streaming from a queue

### Installation
This package __will be__ hosted on PyPI and can be installed via `pip`:
- To install with the ability to process pandas DataFrames:
  ```
  $ pip install elasticbatch[pandas]
  ```
- For a more lightweight installation with only the ability to process native Python dicts:
  ```
  $ pip install elasticbatch
  ```
The only dependency of the latter is `elasticsearch` whereas the former will also install `pandas` as a dependency.

To instead install from source:
```
$ git clone https://github.com/dkaslovsky/ElasticBatch.git
$ cd ElasticBatch
$ pip install ".[pandas]"
```
To install from source without the `pandas` dependency, replace the last line above with
```
$ pip install .
```

### Usage

### Tests
To run tests:
```
$ python -m unittest discover -v
```
I usually run tests with the awesome [green](https://github.com/CleanCut/green) package, which I highly recommend!
```
$ green -vr
```
