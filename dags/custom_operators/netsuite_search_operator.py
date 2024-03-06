from requests_oauthlib import OAuth1Session
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from log import log

@log
class NetSuiteSearchOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        search_type,
        search_id,
        filter_expression = None,
        *args, **kwargs
    ):
        super.__init__(*args, **kwargs)
        self.search_type = search_type
        self.search_id = search_id
        self.filter_expression = [] if filter_expression is None else filter_expression

    def __repr__(self):
        return f'Executing search_id {self.search_id} for {self.search_type}'

    def _add_filters(self):
        pass

    def _check_search_type(self, types):
        return any(t == self.search_type for t in types)
    
    def _check_search_id(self):
        return self.search_id.startswith('customsearch_')
    
    def _check_filters(self):
        return all(
            operator in ('and', 'or')
            for (i, operator) in enumerate(self.filter_expression)
            if i % 2 != 0
        )

    def _validate_search(self):
        checks = {
            "Invalid NetSuite search type": self._check_search_type(['transactions', 'customer']),
            "Malformed script_id property": self._check_search_id,
            "Invalid filter operator separation": self._check_filters,
        }

        for error_message, check_function in checks.items():
            if not check_function():
                raise ValueError(error_message)

    def execute(self):
        self.logger.info(self)
        pass