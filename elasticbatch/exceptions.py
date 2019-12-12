from typing import Union


class ElasticBatchError(Exception):
    """ Base ElasticBatch exception """


class ElasticBufferErrorWrapper(ElasticBatchError):
    """
    Exception used to wrap potentially long error messages so that verbosity can be controlled.
    This is desirable as some errors result in a message that contains the contents of all documents
    included in a rejected bulk insert request to Elasticsearch.
    """


class ElasticBufferFlushError(ElasticBatchError):
    """ Exception raised by elasticbatch.ElasticBuffer when flushing to Elasticsearch """

    truncated_msg = 'Error writing buffer contents (details truncated by verbose flag)'

    def __init__(self, err: Union[str, Exception], verbose: bool) -> None:
        """
        :param err: error message or Exception added as instance variable for caller access
        :param verbose: flag for full (True) or truncated (False) error messages
        """
        self.err = err
        self.verbose = verbose
        super().__init__(err)

    def __str__(self) -> str:

        if isinstance(self.err, str):
            return self.err

        try:
            err_name = f'{self.err.__module__}.{self.err.__class__.__name__}'
        except AttributeError:
            err_name = self.err.__class__.__name__

        if self.verbose:
            return f'{err_name}: {super().__str__()}'
        if isinstance(self.err, ElasticBufferErrorWrapper):
            return f'{err_name}: {self.truncated_msg}'
        return err_name
