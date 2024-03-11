from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from log import log
from suite_connect import (
    SuiteQuery,
    SuiteExport
)

@log
class NetSuiteToS3Operator(BaseOperator):
    template_fields = ('filename',)

    @apply_defaults
    def __init__(
        self,
        search_types,
        search_type,
        search_id,
        conn_id,
        bucket_name,
        filename,
        filter_expression = None,
        columns = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.search_types = search_types
        self.search_type = search_type
        self.search_id = search_id
        self.conn_id = conn_id
        self.bucket_name = bucket_name
        self.filename = filename
        self.filter_expression = [] if filter_expression is None else filter_expression
        self.columns = [] if columns is None else columns

    def __repr__(self):
        return f'Executing search_id {self.search_id} for {self.search_type}'
    
    def _check_search_type(self):
        return any(search_type == self.search_type for search_type in self.search_types)
    
    def _check_search_id(self):
        return self.search_id.startswith('customsearch_')
    
    def _check_filters(self):
        if self.filter_expression:
            return all(
                operator in ('AND', 'OR')
                for (i, operator) in enumerate(self.filter_expression)
                if i % 2 != 0
            )
        return True
    
    def _check_columns(self):
        if self.columns:
            return type(self.columns) == list and self.columns
        return True

    def _validate_search(self):
        checks = {
            "Invalid NetSuite search type": self._check_search_type,
            "Invalid script_id property": self._check_search_id,
            "Invalid filter operator separation": self._check_filters,
            "Invalid columns and must be entered as list": self._check_columns,
        }

        for error_message, check_function in checks.items():
            if not check_function():
                raise ValueError(error_message)
            
    def execute(self, context):
        try:
            self._validate_search()

            extracted_results = SuiteQuery(
                filters=self.filter_expression,
                columns=self.columns
            ).extract_results()

            self.logger.info(extracted_results)

            loaded_s3 = SuiteExport(
                conn_id = self.conn_id,
                bucket_name = self.bucket_name,
                filename = self.filename,
                results = extracted_results
            ).load_to_s3()

        except Exception as err:
            print(err)
            raise err