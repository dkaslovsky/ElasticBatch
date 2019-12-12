import unittest

from elasticsearch import ElasticsearchException

from elasticbatch.exceptions import (ElasticBufferErrorWrapper,
                                     ElasticBufferFlushError)


class TestElasticBufferFlushError(unittest.TestCase):

    def test_str(self):

        class TestCase:
            def __init__(self, err, verbose, expected_str):
                self.err = err
                self.verbose = verbose
                self.expected_str = expected_str

        tests = {
            'string error message with verbose=False': TestCase(
                err='some error',
                verbose=False,
                expected_str='some error',
            ),
            'string error message with verbose=True': TestCase(
                err='some error',
                verbose=True,
                expected_str='some error',
            ),
            'ValueError with verbose=False': TestCase(
                err=ValueError('some error'),
                verbose=False,
                expected_str='ValueError',
            ),
            'ValueError with verbose=True': TestCase(
                err=ValueError('some error'),
                verbose=True,
                expected_str='ValueError: some error',
            ),
            'ElasticsearchException with verbose=False (full module displayed)': TestCase(
                err=ElasticsearchException('some error'),
                verbose=False,
                expected_str='elasticsearch.exceptions.ElasticsearchException',
            ),
            'ElasticsearchException with verbose=True (full module displayed)': TestCase(
                err=ElasticsearchException('some error'),
                verbose=True,
                expected_str='elasticsearch.exceptions.ElasticsearchException: some error',
            ),
            'ElasticBufferErrorWrapper with verbose=False': TestCase(
                err=ElasticBufferErrorWrapper('long errors [err1, err2]'),
                verbose=False,
                expected_str=\
                    f'{ElasticBufferErrorWrapper.__module__}.ElasticBufferErrorWrapper: '
                    f'{ElasticBufferFlushError.truncated_msg}',
            ),
            'ElasticBufferErrorWrapper with verbose=False': TestCase(
                err=ElasticBufferErrorWrapper('long errors [err1, err2]'),
                verbose=True,
                expected_str=\
                    f'{ElasticBufferErrorWrapper.__module__}.ElasticBufferErrorWrapper: '
                    f'long errors [err1, err2]',
            ),
        }

        for test_name, test in tests.items():
            err = ElasticBufferFlushError(test.err, test.verbose)
            self.assertEqual(str(err), test.expected_str, test_name)
