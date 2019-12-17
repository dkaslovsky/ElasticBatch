# ElasticBatch

Elasticsearch buffer for collecting and batch inserting Python data and pandas DataFrames

[![Build Status](https://travis-ci.com/dkaslovsky/ElasticBatch.svg?branch=master)](https://travis-ci.com/dkaslovsky/ElasticBatch)
[![Coverage Status](https://coveralls.io/repos/github/dkaslovsky/ElasticBatch/badge.svg?branch=master)](https://coveralls.io/github/dkaslovsky/ElasticBatch?branch=master)
[![Python 3.6](https://img.shields.io/badge/python-3.6-blue.svg)](https://www.python.org/downloads/release/python-360/)
[![Python 3.7](https://img.shields.io/badge/python-3.7-blue.svg)](https://www.python.org/downloads/release/python-370/)
[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/)

## Overview
`ElasticBatch` makes it easy to efficiently insert batches of data in the form of Python dictionaries or [pandas](https://pandas.pydata.org/) [DataFrames](https://pandas.pydata.org/pandas-docs/stable/getting_started/dsintro.html#dataframe) into Elasticsearch.  An efficient pattern when processing data bound for [Elasticsearch](https://www.elastic.co/products/elasticsearch) is to collect data records ("documents") in a buffer to be bulk-inserted in batches.  `ElasticBatch` provides this functionality to ease the overhead and reduce the code involved in inserting large batches or streams of data into Elasticsearch.

`ElasticBatch` has been tested with Elasticsearch 7.x, but _should_ work with earlier versions.

## Features
`ElasticBatch` implements the following features (see [Usage](#usage) for examples and more details) that allow a user to:
- Work with documents as lists of dicts or as rows of pandas DataFrames
- Add documents to a buffer that will automatically flush (insert its contents to Elasticsearch) when it is full
- Interact with an intuitive interface that handles all of the underlying Elasticsearch client logic on behalf of the user
- Track the elapsed time a document has been in the buffer, allowing a user to flush the buffer at a desired time interval even when it is not full
- Work within a context manager that will automatically flush before exiting, alleviating the need for extra code to ensure all documents are written to the database
- Optionally dump the buffer contents (documents) to a file before exiting due to an uncaught exception
- Automatically add Elasticsearch metadata fields (e.g., `_index`, `_id`) to each document via user-supplied functions

## Installation
This package is hosted on PyPI and can be installed via `pip`:
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

## Usage

### Basic Usage
Start by importing the `ElasticBuffer` class:
```
>>> from elasticbatch import ElasticBuffer
```
`ElasticBuffer` uses sensible defaults when initialized without parameters:
```
>>> esbuf = ElasticBuffer()
```
Alternatively, one can pass any of the following parameters:
- `size`: (`int`) number of documents the buffer can hold before flushing to Elasticsearch; defaults to `5000`.
- `client_kwargs`: (`dict`) configuration passed to the underlying `elasticsearch.Elasticsearch` client; see the Elasticsearch [documentation](https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch) for all available options.
- `bulk_kwargs`: (`dict`) configuration passed to the underlying call to `elasticsearch.helpers.bulk` for bulk insertion; see the Elasticsearch [documentation](https://elasticsearch-py.readthedocs.io/en/master/helpers.html#elasticsearch.helpers.bulk) for all available options.
- `verbose_errs`: (`bool`) whether verbose (`True`, default) or truncated (`False`) exceptions are raised; see [Exception Handling](#exception-handling) for more details.
- `dump_dir`: (`str`) directory to write buffer contents when exiting context due to raised Exception; defaults to `None` for not writing to file.
- `**metadata_funcs`: (`callable`) functions to apply to each document for adding Elasticsearch metadata.; see [Automatic Elasticsearch Metadata Fields](#automatic-elasticsearch-metadata-fields) for more details.

Once initialized, `ElasticBuffer` exposes two methods, `add` and `flush`.
Use `add` to add documents to the buffer, noting that all documents in the buffer will be flushed and inserted into Elasticsearch once the number of docuemnts exceeds the buffer's size:
```
>>> docs = [
        {'_index': 'my-index', 'a': 1, 'b': 2.1, 'c': 'xyz'},
        {'_index': 'my-index', 'a': 3, 'b': 4.1, 'c': 'xyy'},
        {'_index': 'my-other-index', 'a': 5, 'b': 6.1, 'c': 'zzz'},
        {'_index': 'my-other-index', 'a': 7, 'b': 8.1, 'c': 'zyx'},
    ]
>>> esbuf.add(docs)
```
Note that all metadata fields required for indexing into Elasticsearch (e.g., `_index` above) must either be included in each document or added [programmatically](#automatic-elasticsearch-metadata-fields) via callable kwarg parameters supplied to the `ElasticBuffer` instance (see below).

To manually force a buffer flush and insert all documents to Elasticsearch, use the `flush` method which does not accept any arguments:
```
>>> esbuf.flush()
```

### pandas DataFrames

Alternatively, one can directly insert a pandas DataFrame into the buffer and each row will be treated as a document:
```
>>> import pandas as pd
>>> df = pd.DataFrame(docs)
>>> print(df)

           _index  a    b    c
0        my-index  1  2.1  xyz
1        my-index  3  4.1  xyy
2  my-other-index  5  6.1  zzz
3  my-other-index  7  8.1  zyx

>>> esbuf.add(df)
```
The DataFrame's index (referring to `df.index` and __not__ the column `_index`) is ignored unless it is named, in which case it is added as an ordinary field (column).

### Context Manager

`ElasticBuffer` can also be used as a context manager, offering the advantages of automatically flushing the remaining buffer contents when exiting scope as well as optionally dumping the buffer contents to a file before exiting due to an unhandled exception.
```
>>> with ElasticBuffer(size=100, dump_dir='/tmp') as esbuf:
       for doc in document_stream:
           doc = process_document(doc)  # some user-defined application-specific processing function
           esbuf.add(doc)
```

### Elapsed Time

When using `ElasticBuffer` in a service consuming messages from some external source, it can be important to track how long messages have been waiting in the buffer to be flushed.  In particular, a user may wish to flush, say, every hour to account for the situation where only a trickle of data is coming in and the buffer is not filling up.  `ElasticBuffer` provides the elapsed time (in seconds) that its oldest message has been in the buffer:
```
>>> esbuf.oldest_elapsed_time

5.687833070755005  # the oldest message was inserted ~5.69 seconds ago
```
This information can be used to periodically check the elapsed time of the oldest message and force a flush if it exceeds a desired threshold.

### Automatic Elasticsearch Metadata Fields

An `ElasticBuffer` instance can be initialized with kwargs corresponding to callable functions to add [Elasticsearch metadata](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-fields.html) fields to each message added to the buffer:
```
>>> def my_index_func(doc): return 'my-index'
>>> def my_id_func(doc): return sum(doc.values())

>>> esbuf = ElasticBuffer(_index=my_index_func, _id=my_id_func)

>>> docs = [
        {'a': 1, 'b': 2},
        {'a': 8, 'b': 9},
    ]
>>> esbuf.add(docs)

>>> esbuf.show()

{"a": 1, "b": 2, "_index": "my-index", "_id": 3}
{"a": 8, "b": 9, "_index": "my-index", "_id": 17}
```
Callable kwargs add key/value pairs to each document, where the key corresponds to the name of the kwarg and the value is the function's return value.  This works for DataFrames, as they are transformed to documents (dicts) before applying the supplied metadata functions.

The key/value pairs are added to the top level of each document.  Note that the user need not add documents with data nested under a `_source` key, as metadata fields can be handled at the same level as the data fields.  For further details, see the underlying Elasticsearch client [bulk insert](https://elasticsearch-py.readthedocs.io/en/master/helpers.html) documentation on handling of metadata fields in flat dicts.

### Exception Handling

For exception handing, `ElasticBatch` provides the base exception `ElasticBatchError`:
```
>>> from elasticbatch import ElasticBatchError
```
as well as the more specific `ElasticBufferFlushError` raised on errors flushing to Elasticsearch:
```
>>> from elasticbatch.exceptions import ElasticBufferFlushError
```
Elasticsearch exception messages can contain a copy of every document related to a failed bulk insertion request.  As such messages can be very large, the `verbose_errors` flag can be used to optionally truncate the error message.  When `ElasticBuffer` is initialized with `verbose_errors=True`, the entirety of the error message is returned.  When `verbose_errors=False`, a shorter, descriptive message is returned.  In both cases, the full, potentially verbose, exception is available via the `err` property on the raised `ElasticBufferFlushError`.

## Tests
To run tests:
```
$ python -m unittest discover -v
```
The awesome [green](https://github.com/CleanCut/green) package is also highly recommended for running tests and reporting test coverage:
```
$ green -vvr
```
