from typing import List, Optional, Union


class ElasticBatchError(Exception):
    """ Base ElasticBatch exception """


class ElasticBufferFlushError(ElasticBatchError):
    """
    Exception raised by elasticbatch.ElasticBuffer when flushing to Elasticsearch
    Some errors can result in a message that contains the all of the documents from a rejected
    bulk insert request to Elasticsearch. Because this can be quite big, the verbose flag can
    be used to optionally truncate the error message.
    """

    def __init__(
        self,
        msg: Optional[str] = None,
        err: Optional[Union[str, List[str], Exception]] = None,
        verbose: bool = True,
    ) -> None:
        """
        :param msg: error message to display
        :param err: error message(s) or Exception added as instance variable for caller access, also
          used in error message when verbose is True
        :param verbose: flag for full (True) or truncated (False) error messages
        """
        self.msg = msg if msg is not None else ''
        self.err = err
        self.verbose = verbose

    def __str__(self) -> str:
        if not self.verbose:
            return self.msg
        if self.err is None:
            return self.msg
        if isinstance(self.err, (str, list)):
            return f'{self.msg}: {self.err}'

        try:
            err_name = f'{self.err.__module__}.{self.err.__class__.__name__}'
        except AttributeError:
            err_name = self.err.__class__.__name__
        return f'{self.msg}: {err_name}: {self.err}'
