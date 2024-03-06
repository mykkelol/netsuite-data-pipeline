from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class NetSuiteSearchOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        type,
        searchId,
        *args, **kwargs
    ):
        super.__init__(*args, **kwargs)
        self.type = type
        self.searchId = searchId