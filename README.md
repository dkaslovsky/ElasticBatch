# ElasticBatch

[![Build Status](https://travis-ci.com/dkaslovsky/ElasticBatch.svg?branch=master)](https://travis-ci.com/dkaslovsky/ElasticBatch)
[![Coverage Status](https://coveralls.io/repos/github/dkaslovsky/ElasticBatch/badge.svg?branch=master)](https://coveralls.io/github/dkaslovsky/ElasticBatch?branch=master)
[![Python 3.6](https://img.shields.io/badge/python-3.6-blue.svg)](https://www.python.org/downloads/release/python-360/)
[![Python 3.7](https://img.shields.io/badge/python-3.7-blue.svg)](https://www.python.org/downloads/release/python-370/)
[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/)

Elasticsearch buffer for collecting and batch inserting Python data and pandas DataFrames

### Updates
2019-12-14: Work in progress nearing completion; full description to come

### Overview
An efficient pattern when processing data bound for [Elasticsearch](https://www.elastic.co/products/elasticsearch) is to collect data records ("documents") in a buffer to be bulk-inserted into Elasticsearch in a single batch.  `ElasticBatch` provides this functionality to ease the overhead and code involved in inserting large batches or streams of data into Elasticsearch.  In particular, `ElasticBatch` makes it easy to efficiently insert batches of data in the form of Python dictionaries or [pandas](https://pandas.pydata.org/) [DataFrames](https://pandas.pydata.org/pandas-docs/stable/getting_started/dsintro.html#dataframe) into Elasticsearch.
