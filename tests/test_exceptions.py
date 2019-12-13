import unittest

from elasticsearch import ElasticsearchException

from elasticbatch.exceptions import ElasticBufferFlushError


class TestElasticBufferFlushError(unittest.TestCase):

    def test_str(self):

        class TestCase:
            def __init__(self, msg, err, verbose, expected_str):
                self.msg = msg
                self.err = err
                self.verbose = verbose
                self.expected_str = expected_str

        tests = {
            'msg and err are None, verbose=False': TestCase(
                msg=None,
                err=None,
                verbose=False,
                expected_str=''
            ),
            'msg and err are None, verbose=True': TestCase(
                msg=None,
                err=None,
                verbose=True,
                expected_str=''
            ),
            'msg only, verbose=False': TestCase(
                msg='error message',
                err=None,
                verbose=False,
                expected_str='error message',
            ),
            'msg only, verbose=True': TestCase(
                msg='error message',
                err=None,
                verbose=True,
                expected_str='error message',
            ),
            'err is string, verbose=False': TestCase(
                msg='error message',
                err='we have a big problem',
                verbose=False,
                expected_str='error message',
            ),
            'err is string, verbose=True': TestCase(
                msg='error message',
                err='we have a big problem',
                verbose=True,
                expected_str='error message: we have a big problem',
            ),
            'err is list, verbose=False': TestCase(
                msg='error message',
                err=['error1', 'error2', 'error3'],
                verbose=False,
                expected_str='error message',
            ),
            'err is list, verbose=True': TestCase(
                msg='error message',
                err=['error1', 'error2', 'error3'],
                verbose=True,
                expected_str='error message: [\'error1\', \'error2\', \'error3\']',
            ),
            'err is ValueError, verbose=False': TestCase(
                msg='error message',
                err=ValueError('we have a big problem'),
                verbose=False,
                expected_str='error message',
            ),
            'err is ValueError, verbose=True': TestCase(
                msg='error message',
                err=ValueError('we have a big problem'),
                verbose=True,
                expected_str='error message: ValueError: we have a big problem',
            ),
            'err is ElasticsearchException, verbose=False': TestCase(
                msg='error message',
                err=ElasticsearchException('we have a big problem'),
                verbose=False,
                expected_str='error message',
            ),
            'err is ElasticsearchException, verbose=True': TestCase(
                msg='error message',
                err=ElasticsearchException('we have a big problem'),
                verbose=True,
                expected_str='error message: elasticsearch.exceptions.ElasticsearchException: '
                             'we have a big problem',
            ),
        }

        for test_name, test in tests.items():
            err = ElasticBufferFlushError(msg=test.msg, err=test.err, verbose=test.verbose)
            self.assertEqual(str(err), test.expected_str, test_name)
