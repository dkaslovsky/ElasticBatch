import unittest
from unittest.mock import patch

from elasticsearch import ElasticsearchException

from src.insert import ElasticBuffer, ElasticBufferException

# pylint: disable=protected-access


class TestElasticBuffer(unittest.TestCase):

    docs = [
        {'a': 1, 'b': 2},
        {'a': 3, 'b': 4},
        {'a': 5, 'b': 6},
    ]

    timestamp = 123.456

    @patch(f'{ElasticBuffer.__module__}.bulk')
    def test_flush_empty_buffer(self, mock_bulk):
        eb = ElasticBuffer()
        eb.flush()
        mock_bulk.assert_not_called()

    @patch(f'{ElasticBuffer.__module__}.bulk')
    def test_flush_success(self, mock_bulk):
        mock_bulk.return_value = (len(self.docs), [])

        eb = ElasticBuffer()
        eb._buffer = self.docs
        eb._oldest_doc_timestamp = self.timestamp
        eb.flush()

        # assert contents of buffer were passed to bulk
        (_, called_docs), _ = mock_bulk.call_args
        self.assertListEqual(called_docs, self.docs, 'contents of buffer should have been passed to bulk')

        # assert state was cleared
        self.assertListEqual(eb._buffer, [], 'buffer should be empty after successful insert')
        self.assertIsNone(eb._oldest_doc_timestamp, 'timestamp should be None after successful insert')

    @patch(f'{ElasticBuffer.__module__}.bulk')
    def test_flush_error(self, mock_bulk):

        class TestCase:
            def __init__(self, n_success=0, bulk_errs=None, side_effect=None):
                self.eb = ElasticBuffer()
                self.eb._buffer = TestElasticBuffer.docs
                self.eb._oldest_doc_timestamp = TestElasticBuffer.timestamp

                self.return_value = (n_success, bulk_errs)
                self.side_effect = side_effect

        tests = {
            'bulk raises exception': TestCase(
                side_effect=ElasticsearchException,
            ),
            'not all docs successfully inserted': TestCase(
                n_success=len(self.docs)-1,
                bulk_errs=[],
            ),
            'error returned': TestCase(
                n_success=len(self.docs),
                bulk_errs=['err1'],
            ),
        }

        for test_name, test in tests.items():
            mock_bulk.reset_mock()
            mock_bulk.return_value = test.return_value
            mock_bulk.side_effect = test.side_effect

            with self.assertRaises(ElasticBufferException, msg=test_name):
                test.eb.flush()

            # assert state was not cleared
            self.assertListEqual(test.eb._buffer, self.docs, test_name)
            self.assertEqual(test.eb._oldest_doc_timestamp, self.timestamp, test_name)


    def test__clear_buffer(self):

        class TestCase:
            def __init__(self, buf, timestamp):
                self.eb = ElasticBuffer()
                if buf:
                    self.eb._buffer = buf
                if timestamp:
                    self.eb._oldest_doc_timestamp = timestamp

        tests = {
            'empty buffer': TestCase(
                buf=[],
                timestamp=None,
            ),
            'nonempty buffer': TestCase(
                buf=self.docs,
                timestamp=123.456
            )
        }

        for test_name, test in tests.items():

            test.eb._clear_buffer()
            self.assertListEqual(test.eb._buffer, [], test_name)
            self.assertIsNone(test.eb._oldest_doc_timestamp, test_name)

    def test_len(self):

        class TestCase:
            def __init__(self, n_items):
                self.n_items = n_items
                self.eb = ElasticBuffer()
                if n_items > 0:
                    self.eb._buffer = ['a'] * n_items

        tests = {
            'empty buffer': TestCase(n_items=0),
            'one item in buffer': TestCase(n_items=1),
            'multiple items in buffer': TestCase(n_items=1234),
        }

        for test_name, test in tests.items():
            self.assertEqual(len(test.eb), test.n_items, test_name)
