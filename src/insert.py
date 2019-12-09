import time
from typing import Dict, List, Optional, Union

from elasticsearch import Elasticsearch, ElasticsearchException
from elasticsearch.helpers import bulk


class ElasticBuffer:

    def __init__(
        self,
        size: int = 5000,
        client_args: Optional[Dict] = None,
        bulk_args: Optional[Dict] = None,
    ) -> None:
        """
        :param size: number of documents buffer can hold before flushing to Elasticsearch
        :param client_args: dict of kwargs for elasticsearch.Elasticsearch client configuration
        :param bulk_args: dict of kwargs for elasticsearch.helpers.bulk insertion
        """

        self.size = size

        self.client = Elasticsearch(**client_args) if client_args else Elasticsearch()

        self.bulk_args = {
            'chunk_size': size,  # always bulk insert this many documents at a time
            'max_retries': 3,    # number of retries in case of insertion error
        }
        if bulk_args:
            self.bulk_args.update(bulk_args)  # bulk_args can overwrite the above defaults

        self._buffer = []                  # type: List[Dict]
        self._oldest_doc_timestamp = None  # type: Optional[float]

    def __str__(self):
        """
        Printable class information
        """
        return f'{self.__class__.__name__} containing {len(self)} documents'

    def __len__(self):
        """
        Return number of documents in buffer
        """
        return len(self._buffer)

    def add(self, docs: Union[List[Dict], Dict], timestamp: Optional[float] = None) -> None:
        """
        Add documents to buffer
        :param docs: documents to append
        """
        if not isinstance(docs, list):
            docs = [docs]
        if not docs:
            return

        # record timestamp of insert time if buffer is empty
        if len(self) == 0:
            now = time.time() if timestamp is None else timestamp
            self._oldest_doc_timestamp = now

        self._buffer.extend(docs)

        # flush if buffer is full
        if len(self) > self.size:
            self.flush()

    def flush(self) -> None:
        """
        Bulk insert buffer contents to Elasticsearch
        """
        if len(self) == 0:
            return

        try:
            n_success, bulk_errs = bulk(self.client, self._buffer, **self.bulk_args)
        except ElasticsearchException as err:
            raise ElasticBufferException(err)
        if len(bulk_errs) != 0:
            # TODO HANDLE VERBOSITY HERE
            raise ElasticBufferException(f'Bulk insertion errrors: {bulk_errs}')
        if n_success != len(self):
            n_fail = len(self) - n_success
            raise ElasticBufferException(f'Failed to insert {n_fail} of {len(self)} documents')

        # clear buffer on successfull bulk insert
        self._clear_buffer()

    def _clear_buffer(self) -> None:
        """
        Clear buffer contents and associated state
        """
        self._buffer = []
        self._oldest_doc_timestamp = None


class ElasticBufferException(Exception):
    pass
