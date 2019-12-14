# ElasticBatch

[![Build Status](https://travis-ci.com/dkaslovsky/ElasticBatch.svg?branch=master
)](https://travis-ci.com/dkaslovsky/ElasticBatch)
[![Coverage Status](https://coveralls.io/repos/github/dkaslovsky/ElasticBatch/badge.svg?branch=master)](https://coveralls.io/github/dkaslovsky/ElasticBatch?branch=master)

Elasticsearch bulk writer for batch processing

Implementation of a pattern for bulk insertion of documents into Elasticsearch useful when
batch processing data

Features
* context manager
* easily handle dataframes
* track oldest insert time (show example pattern)
