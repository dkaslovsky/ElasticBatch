from __future__ import annotations

import json
import math
import time
from typing import Any, Dict, List, Optional, Type, Union, types

import pandas as pd
from elasticsearch import Elasticsearch, ElasticsearchException
from elasticsearch.helpers import bulk

from elasticbatch.exceptions import (ElasticBufferErrorWrapper,
                                     ElasticBufferFlushError)

DocumentBundle = Union[Dict, List[Dict], pd.Series, pd.DataFrame]

class ElasticBuffer:

    def __init__(
        self,
        size: int = 5000,
        client_kwargs: Optional[Dict[str, Any]] = None,
        bulk_kwargs: Optional[Dict[str, Any]] = None,
        dump_on_err: bool = False,
        verbose_errs: bool = True,
    ) -> None:
        """
        :param size: number of documents buffer can hold before flushing to Elasticsearch
        :param client_kwargs: dict of kwargs for elasticsearch.Elasticsearch client configuration
        :param bulk_kwargs: dict of kwargs for elasticsearch.helpers.bulk insertion
        :param dump_on_err: whether to write buffer to a file on exception in context manager
        :param verbose_errs: whether full (True; default) or truncated (False) errors are raised
        """

        self.size = size
        self.verbose_errs = verbose_errs
        self.dump_on_err = dump_on_err

        self.bulk_kwargs = self._construct_bulk_kwargs(size, bulk_kwargs)

        self._client = Elasticsearch(**client_kwargs) if client_kwargs else Elasticsearch()

        self._buffer = []                  # type: List[Dict]
        self._oldest_doc_timestamp = None  # type: Optional[float]

    def __str__(self) -> str:
        """
        Printable class information
        """
        return f'{self.__class__.__name__} containing {len(self)} documents'

    def __len__(self) -> int:
        """
        Return number of documents in buffer
        """
        return len(self._buffer)

    def __enter__(self) -> ElasticBuffer:
        """
        Enable context manager
        """
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        """
        Ensure flush is called when exiting context unless exception is raised
        :param exc_type: type of any exception raised inside the context
        :param exc_val: value of any exception raised inside the context
        :param exc_tb: Traceback of any exception raised inside the context
        """
        err_raised = any((exc_type, exc_val, exc_tb))
        # only flush if exiting without raised Exception
        if not err_raised:
            self.flush()
            return
        # write contents of buffer to file on Exception
        if self.dump_on_err:
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
            raise ElasticBufferFlushError(err, self.verbose_errs)

        if len(bulk_errs) != 0:
            raise ElasticBufferFlushError(
                ElasticBufferErrorWrapper(f'Bulk insertion errrors: {bulk_errs}'),
                self.verbose_errs,
            )
        if n_success != len(self):
            n_fail = len(self) - n_success
            raise ElasticBufferFlushError(
                f'Failed to insert {n_fail} of {len(self)} documents',
                self.verbose_errs,
            )

        # clear buffer on successfull bulk insert
        self._clear_buffer()

    def add(self, docs: DocumentBundle, timestamp: Optional[float] = None) -> None:
        """
        Add documents from an DocumentBundle data structure to buffer
        :param docs: DocumentBundle of documents to append
        :param timestamp: seconds from epoch to associate as insert time for docs; defaults to now
        """
        docs_list = self._ensure_list(docs)

        timestamp = time.time() if timestamp is None else timestamp
        self._add(docs_list, timestamp)

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

    def _clear_buffer(self) -> None:
        """
        Clear buffer contents and associated state
        """
        self._buffer = []
        self._oldest_doc_timestamp = None

    def _to_file(self, timestamp: Optional[float] = None):
        """
        Write contents of buffer as ndjson file
        """
        if len(self) == 0:
            return
        timestamp = time.time() if timestamp is None else timestamp
        file_name = f'{self.__class__.__name__}_buffer_dump_{timestamp}'
        with open(file_name, 'w') as handle:
            for doc in self._buffer:
                handle.write(json.dumps(doc) + '\n')

    @staticmethod
    def _ensure_list(docs: DocumentBundle) -> List[Dict]:
        if isinstance(docs, list):
            return docs
        if isinstance(docs, dict):
            return [docs]
        if isinstance(docs, pd.Series):
            docs = docs.to_frame()
        if isinstance(docs, pd.DataFrame):
            if docs.index.name:
                docs = docs.reset_index()
            return docs.to_dict(orient='records')
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
