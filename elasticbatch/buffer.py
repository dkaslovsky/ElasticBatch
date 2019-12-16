import json
import math
import os
import time
from typing import Any, Callable, Dict, List, Optional

from elasticsearch import Elasticsearch, ElasticsearchException
from elasticsearch.helpers import bulk

from elasticbatch.exceptions import ElasticBufferFlushError
from elasticbatch.types import DocumentBundle, no_pandas


class ElasticBuffer:

    def __init__(
        self,
        size: int = 5000,
        client_kwargs: Optional[Dict[str, Any]] = None,
        bulk_kwargs: Optional[Dict[str, Any]] = None,
        verbose_errs: bool = True,
        dump_dir: Optional[str] = None,
        **metadata_funcs: Callable[[Dict], Any],
    ) -> None:
        """
        :param size: number of documents buffer can hold before flushing to Elasticsearch
        :param client_kwargs: dict of kwargs for elasticsearch.Elasticsearch client configuration
        :param bulk_kwargs: dict of kwargs for elasticsearch.helpers.bulk insertion
        :param verbose_errs: whether full (True; default) or truncated (False) errors are raised
        :param dump_dir: directory to write buffer contents when exiting context due to raised
          exception; pass None to not write to file (default)
        :param metadata_funcs: optional functions for generating Elasticsearch metadata fields
          (e.g., _index, _id) that will be appended to the top level of every document. Each
          function must accept one argument (the document as a dict) and return one value.
          The resulting document will contain a new key/value pair corresponding to the function
          name (key) and function return (value). For the case of DataFrame input, the functions
          are applied to the documents generated from the DataFrame. It is generally more efficient
          to add documents already containing these metadata fields rather than generating metadata
          via these functions.
        """

        self.size = size
        self.verbose_errs = verbose_errs
        self.dump_dir = dump_dir
        self.metadata_funcs = metadata_funcs

        self.bulk_kwargs = self._construct_bulk_kwargs(size, bulk_kwargs)

        self._client = Elasticsearch(**client_kwargs) if client_kwargs else Elasticsearch()

        self._buffer = []                  # type: List[Dict]
        self._oldest_doc_timestamp = None  # type: Optional[float]

    def __str__(self):
        return f'{self.__class__.__name__} containing {len(self)} documents'

    def __len__(self):
        return len(self._buffer)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        err_raised = any((exc_type, exc_val, exc_tb))
        # only flush if exiting without raised Exception
        if not err_raised:
            self.flush()
            return
        # write contents of buffer to file on Exception
        if self.dump_dir:
            self._to_file()

    @property
    def oldest_elapsed_time(self) -> float:
        """
        Get elapsed time in seconds between now and insert time of oldest document in buffer
        """
        now = time.time()
        return self._get_oldest_elapsed_time_from(now)

    def flush(self) -> None:
        """
        Bulk insert buffer contents to Elasticsearch
        """
        if len(self) == 0:
            return

        try:
            n_success, bulk_errs = bulk(self._client, self._buffer, **self.bulk_kwargs)
        except ElasticsearchException as err:
            raise ElasticBufferFlushError(
                msg='Error while bulk inserting buffer contents',
                err=err,
                verbose=self.verbose_errs,
            )

        if len(bulk_errs) != 0:
            raise ElasticBufferFlushError(
                msg='Multiple bulk insertion errors',
                err=bulk_errs,
                verbose=self.verbose_errs,
            )
        if n_success != len(self):
            n_fail = len(self) - n_success
            raise ElasticBufferFlushError(
                msg=f'Failed to insert {n_fail} of {len(self)} documents',
                verbose=self.verbose_errs,
            )

        # clear buffer on successful bulk insert
        self._clear_buffer()

    def add(self, docs: DocumentBundle, timestamp: Optional[float] = None) -> None:
        """
        Add documents from an DocumentBundle data structure to buffer
        :param docs: DocumentBundle of documents to append
        :param timestamp: seconds from epoch to associate as insert time for docs; defaults to now
        """
        docs_list = self._ensure_list(docs)
        docs_list = self._apply_metadata_funcs(docs_list)
        timestamp = time.time() if timestamp is None else timestamp
        self._add(docs_list, timestamp)

    def show(self) -> None:
        """
        Print each (json-serialized) document in the buffer on a new line
        """
        for doc in self._buffer:
            print(json.dumps(doc))

    def _add(self, docs: List[Dict], timestamp: float) -> None:
        """
        Add list of documents to buffer
        :param docs: documents to append
        :param timestamp: seconds from epoch to associate as insert time for docs
        """
        if not docs:
            return

        # record timestamp of insert time if buffer is empty
        if len(self) == 0:
            self._oldest_doc_timestamp = timestamp

        self._buffer.extend(docs)

        # flush if buffer is full
        if len(self) > self.size:
            self.flush()

    def _apply_metadata_funcs(self, docs: List[Dict]) -> List[Dict]:
        """
        Return list of documents updated with the result of metadata functions
        :param docs: documents on which to apply metadata functions
        """
        if not self.metadata_funcs:
            return docs
        for doc in docs:
            doc.update({field: func(doc) for field, func in self.metadata_funcs.items()})
        return docs

    def _clear_buffer(self) -> None:
        """
        Clear buffer contents and associated state
        """
        self._buffer = []
        self._oldest_doc_timestamp = None

    def _to_file(self, timestamp: Optional[float] = None):
        """
        Write contents of buffer as ndjson file
        :param timestamp: timestamp to associate with dumped file; defaults to now
        """
        timestamp = time.time() if timestamp is None else timestamp
        dump_file = os.path.join(
            self.dump_dir,  # type: ignore  # function not called when None
            f'{self.__class__.__name__}_buffer_dump_{timestamp}'
        )
        with open(dump_file, 'w') as handle:
            for doc in self._buffer:
                handle.write(json.dumps(doc) + '\n')

    def _get_oldest_elapsed_time_from(self, timestamp: float) -> float:
        """
        Return elapsed seconds between timestamp and insert time of oldest document in the buffer
        :param timestamp: timestamp in seconds (usually from epoch)
        """
        if self._oldest_doc_timestamp is None:
            return -math.inf
        try:
            return timestamp - self._oldest_doc_timestamp
        except TypeError:
            raise TypeError('Cannot use non-float as numeric value for computing elapsed time')

    @staticmethod
    def _ensure_list(docs: DocumentBundle) -> List[Dict]:
        if isinstance(docs, list):
            return docs
        if isinstance(docs, dict):
            return [docs]
        if no_pandas:
            raise ValueError('Must pass one of [List, Dict]')

        # docs is a pandas Series
        try:
            docs = docs.to_frame()
        except AttributeError:
            pass

        # docs is a pandas DataFrame
        try:
            if docs.index.name:
                docs = docs.reset_index()
            return docs.to_dict(orient='records')
        except AttributeError:
            pass
        raise ValueError('Must pass one of [List, Dict, pandas.Series, pandas.DataFrame]')

    @staticmethod
    def _construct_bulk_kwargs(size: int, bulk_kwargs: Optional[Dict]) -> Dict[str, Any]:
        """
        Construct Dict of kwargs to be passed to bulk
        :param size: number of documents the underlying client should bulk insert at a time
        :param bulk_kwargs: optional dict of kwargs to pass to bulk; values set in this parameter
         will overwrite defaults set below
        """
        bulk_kwargs = bulk_kwargs if bulk_kwargs is not None else {}
        return {
            'max_retries': 3,  # number of retries in case of insertion error
            'chunk_size': size,
            **bulk_kwargs,
        }
