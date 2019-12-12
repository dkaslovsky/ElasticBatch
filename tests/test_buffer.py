import json
import math
import unittest
from unittest.mock import mock_open, patch

import pandas as pd
from elasticsearch import ElasticsearchException

from elasticbatch.buffer import ElasticBuffer
from elasticbatch.exceptions import ElasticBatchError

# pylint: disable=protected-access


class TestElasticBuffer(unittest.TestCase):

    docs = [
        {'a': 1, 'b': 2.1, 'c': 'xyz'},
        {'a': 3, 'b': 4.1, 'c': 'xyy'},
        {'a': 5, 'b': 6.1, 'c': 'zzz'},
        {'a': 7, 'b': 8.1, 'c': 'zyx'},
    ]

    timestamp = 123.456

    @patch.object(ElasticBuffer, 'flush')
    def test__add(self, mock_flush):

        class TestCase:
            def __init__(
                self,
                documents,
                documents_timestamp,
                expected_buffer,
                expected_oldest_doc_timestamp,
                expected_flush_called,
                buffer_size=10,
                # used for multiple adds
                more_documents=None,
                more_documents_timestamp=None,
            ):
                self.documents = documents
                self.documents_timestamp = documents_timestamp

                self.expected_buffer = expected_buffer
                self.expected_oldest_doc_timestamp = expected_oldest_doc_timestamp
                self.expected_flush_called = expected_flush_called

                self.more_documents = [] if more_documents is None else more_documents
                self.more_documents_timestamp = None if more_documents_timestamp is None else more_documents_timestamp

                self.eb = ElasticBuffer(size=buffer_size)

        tests = {
            'add empty list of records': TestCase(
                documents=[],
                documents_timestamp=1234,
                expected_buffer=[],
                expected_oldest_doc_timestamp=None,
                expected_flush_called=False,
            ),
            'add single record in a list': TestCase(
                documents=[self.docs[0]],
                documents_timestamp=1234,
                expected_buffer=[self.docs[0]],
                expected_oldest_doc_timestamp=1234,
                expected_flush_called=False,
            ),
            'add list of records below buffer size': TestCase(
                documents=self.docs,
                documents_timestamp=1234,
                expected_buffer=self.docs,
                expected_oldest_doc_timestamp=1234,
                expected_flush_called=False,
                buffer_size=len(self.docs) + 1,
            ),
            'add list of records equal to buffer size': TestCase(
                documents=self.docs,
                documents_timestamp=1234,
                expected_buffer=self.docs,
                expected_oldest_doc_timestamp=1234,
                expected_flush_called=False,
                buffer_size=len(self.docs),
            ),
            'add list of records exceeding buffer size': TestCase(
                documents=self.docs,
                documents_timestamp=1234,
                expected_buffer=[],
                expected_oldest_doc_timestamp=None,
                expected_flush_called=True,
                buffer_size=len(self.docs) - 1,
            ),
            'add twice without exceeding buffer size': TestCase(
                documents=self.docs[:-1],
                documents_timestamp=1234,
                more_documents=[self.docs[-1]],
                more_documents_timestamp=1235,
                expected_buffer=self.docs,
                expected_oldest_doc_timestamp=1234,
                expected_flush_called=False,
                buffer_size=len(self.docs),
            ),
            'add twice with first add exceeding buffer size': TestCase(
                documents=self.docs[:-1],
                documents_timestamp=1234,
                more_documents=[self.docs[-1]],
                more_documents_timestamp=1235,
                expected_buffer=[self.docs[-1]],
                expected_oldest_doc_timestamp=1235,
                expected_flush_called=True,
                buffer_size=len(self.docs) - 2,
            ),
            'add twice with second add exceeding buffer size': TestCase(
                documents=self.docs[:-1],
                documents_timestamp=1234,
                more_documents=[self.docs[-1]],
                more_documents_timestamp=1235,
                expected_buffer=[],
                expected_oldest_doc_timestamp=None,
                expected_flush_called=True,
                buffer_size=len(self.docs) - 1,
            ),
            'add twice with both adds exceeding buffer size': TestCase(
                documents=self.docs,
                documents_timestamp=1234,
                more_documents=self.docs,
                more_documents_timestamp=1235,
                expected_buffer=[],
                expected_oldest_doc_timestamp=None,
                expected_flush_called=True,
                buffer_size=1,
            ),
        }

        for test_name, test in tests.items():
            mock_flush.reset_mock()
            mock_flush.side_effect = test.eb._clear_buffer

            test.eb._add(test.documents, timestamp=test.documents_timestamp)
            if test.more_documents:
                test.eb._add(test.more_documents, timestamp=test.more_documents_timestamp)
            
            self.assertListEqual(test.eb._buffer, test.expected_buffer, test_name)
            self.assertEqual(test.eb._oldest_doc_timestamp, test.expected_oldest_doc_timestamp, test_name)
            if test.expected_flush_called:
                mock_flush.assert_called()
            else:
                mock_flush.assert_not_called()

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
            'bulk raises ElasticsearchException': TestCase(
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

            with self.assertRaises(ElasticBatchError, msg=test_name):
                test.eb.flush()

            # assert state was not cleared
            self.assertListEqual(test.eb._buffer, self.docs, test_name)
            self.assertEqual(test.eb._oldest_doc_timestamp, self.timestamp, test_name)

    def test__ensure_list(self):

        series_list = [doc['c'] for doc in self.docs]

        class TestCase:
            def __init__(self, docs_in, expected_docs):
                self.docs_in = docs_in
                self.expected_docs = expected_docs

        tests = {
            'dict': TestCase(
                docs_in=self.docs[0],
                expected_docs=[self.docs[0]],
            ),
            'list': TestCase(
                docs_in=self.docs,
                expected_docs=self.docs,
            ),
            'series': TestCase(
                docs_in=pd.Series(series_list),
                expected_docs=[{0: item} for item in series_list],
            ),
            'named series': TestCase(
                docs_in=pd.Series(series_list).rename('my_name'),
                expected_docs=[{'my_name': item} for item in series_list],
            ),
            'series with named index': TestCase(
                docs_in=pd.Series(series_list).rename_axis('my_axis', axis=0),
                expected_docs=[{0: item, 'my_axis': i} for i, item in enumerate(series_list)],
            ),
            'named series with named index': TestCase(
                docs_in=pd.Series(series_list).rename_axis('my_axis', axis=0).rename('my_name'),
                expected_docs=[{'my_name': item, 'my_axis': i} for i, item in enumerate(series_list)],
            ),
            'series with single row': TestCase(
                docs_in=pd.Series(series_list[0]),
                expected_docs=[{0: item} for item in [series_list[0]]],
            ),
            'named series with single row': TestCase(
                docs_in=pd.Series(series_list[0]).rename('my_name'),
                expected_docs=[{'my_name': item} for item in [series_list[0]]],
            ),
            'named series with single row with named index': TestCase(
                docs_in=pd.Series(series_list[0]).rename_axis('my_index', axis=0).rename('my_name'),
                expected_docs=[{'my_name': item, 'my_index': i} for i, item in enumerate([series_list[0]])],
            ),
            'dataframe': TestCase(
                docs_in=pd.DataFrame(self.docs),
                expected_docs=self.docs,
            ),
            'dataframe with named index': TestCase(
                docs_in=pd.DataFrame(self.docs).set_index('c'),
                expected_docs=self.docs,
            ),
            'dataframe with single row': TestCase(
                docs_in=pd.DataFrame(self.docs[0], index=[0]),
                expected_docs=[self.docs[0]],
            ),
            'dataframe with single row and named index': TestCase(
                docs_in=pd.DataFrame(self.docs[0], index=[0]).set_index('c'),
                expected_docs=[self.docs[0]],
            ),
        }

        for test_name, test in tests.items():
            docs_out = ElasticBuffer._ensure_list(test.docs_in)
            self.assertListEqual(docs_out, test.expected_docs, test_name)

    @patch.object(ElasticBuffer, 'flush')
    def test_context_success(self, mock_flush):

        class TestCase:
            def __init__(self, n_docs, buffer_size, n_expected_flush_calls):
                self.n_docs = n_docs
                self.buffer_size = buffer_size
                self.n_expected_flush_calls = n_expected_flush_calls

        tests = {
            'flush is called on exit with empty buffer': TestCase(
                n_docs=0,
                buffer_size=10,
                n_expected_flush_calls=1,
            ),
            'flush is called on exit with populated buffer': TestCase(
                n_docs=5,
                buffer_size=10,
                n_expected_flush_calls=1,
            ),
            'flush is called once when buffer is full and once on exit': TestCase(
                n_docs=5,
                buffer_size=2,
                n_expected_flush_calls=2,
            ),
        }

        for test_name, test in tests.items():
            mock_flush.reset_mock()

            docs = ['a'] * test.n_docs
            with ElasticBuffer(size=test.buffer_size) as eb:
                mock_flush.side_effect = eb._clear_buffer
                eb.add(docs)
            self.assertEqual(mock_flush.call_count, test.n_expected_flush_calls, test_name)

    @patch.object(ElasticBuffer, 'flush')
    def test_context_error(self, mock_flush):

        class TestCase:
            def __init__(self, n_docs, buffer_size, n_expected_flush_calls):
                self.n_docs = n_docs
                self.buffer_size = buffer_size
                self.n_expected_flush_calls = n_expected_flush_calls

        tests = {
            'flush is not called on exit due to exception with empty buffer': TestCase(
                n_docs=0,
                buffer_size=10,
                n_expected_flush_calls=0,
            ),
            'flush is not called on exit due to exception with populated buffer': TestCase(
                n_docs=5,
                buffer_size=10,
                n_expected_flush_calls=0,
            ),
            'flush is called once when buffer is full but not again on exit': TestCase(
                n_docs=5,
                buffer_size=2,
                n_expected_flush_calls=1,
            ),
        }

        for test_name, test in tests.items():
            mock_flush.reset_mock()
            mock_flush.side_effect = ElasticBatchError

            docs = ['a'] * test.n_docs
            with self.assertRaises((ElasticBatchError, ValueError), msg=test_name):
                with ElasticBuffer(size=test.buffer_size) as eb:
                    eb.add(docs)
                    raise ValueError()
            self.assertEqual(mock_flush.call_count, test.n_expected_flush_calls, test_name)

    def test__to_file(self):
        docs = [{'a': 1}, {'b': 2}]
        content = json.dumps(docs[0]) + '\n' + json.dumps(docs[1]) + '\n'
        with patch('elasticbatch.buffer.open', mock_open()) as mocked_file:
            eb = ElasticBuffer()
            eb.add(docs)
            eb._to_file(timestamp=self.timestamp)

            file_name = f'{eb.__class__.__name__}_buffer_dump_{self.timestamp}'
            mocked_file.assert_called_once_with(file_name, 'w')
            #mocked_file().write.assert_called_with(content)
            for doc in docs:
                mocked_file().write.assert_called_with(json.dumps(doc) + '\n')
            self.assertEqual(mocked_file().write.call_count, len(docs), 'msg')


    def test__get_oldest_elapsed_time_from(self):

        class TestCase:
            def __init__(self, oldest_doc_timestamp, timestamp, expected=None):
                self.timestamp = timestamp
                self.expected = expected

                self.eb = ElasticBuffer()
                self.eb._oldest_doc_timestamp = oldest_doc_timestamp

        tests_success = {
            'oldest time is None (corresponding to empty buffer)': TestCase(
                oldest_doc_timestamp=None,
                timestamp=123.456,
                expected=-math.inf,
            ),
            'oldest time is None (corresponding to empty buffer) with invalid timestamp': TestCase(
                oldest_doc_timestamp=None,
                timestamp='123.456',
                expected=-math.inf,
            ),
            'compute elapsed time from specified time': TestCase(
                oldest_doc_timestamp=100.41,
                timestamp=123.45,
                expected=23.04,
            ),
        }

        tests_error = {
            'string input': TestCase(
                oldest_doc_timestamp=1234,
                timestamp='1235',
            ),
            'None input': TestCase(
                oldest_doc_timestamp=1234,
                timestamp=None,
            ),
        }

        for test_name, test in tests_success.items():
            result = test.eb._get_oldest_elapsed_time_from(test.timestamp)
            self.assertAlmostEqual(result, test.expected, places=3, msg=test_name)

        for test_name, test in tests_error.items():
            with self.assertRaises(TypeError, msg=test_name):
                _ = test.eb._get_oldest_elapsed_time_from(test.timestamp)

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
