from requests_oauthlib import OAuth1Session
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable

from log import log

@log
class NetSuiteSearchOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        search_types,
        search_type,
        search_id,
        filter_expression = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.search_types = search_types
        self.search_type = search_type
        self.search_id = search_id
        self.filter_expression = [] if filter_expression is None else filter_expression

    def __repr__(self):
        return f'Executing search_id {self.search_id} for {self.search_type}'

    def _add_filters(self):
        pass

    def _check_search_type(self):
        return any(search_type == self.search_type for search_type in self.search_types)
    
    def _check_search_id(self):
        return self.search_id.startswith('customsearch_')
    
    def _check_filters(self):
        if not self.filter_expression:
            return all(
                operator in ('and', 'or')
                for (i, operator) in enumerate(self.filter_expression)
                if i % 2 != 0
            )
        return True

    def _validate_search(self):
        checks = {
            "Invalid NetSuite search type": self._check_search_type,
            "Malformed script_id property": self._check_search_id,
            "Invalid filter operator separation": self._check_filters,
        }

        for error_message, check_function in checks.items():
            if not check_function():
                raise ValueError(error_message)
            
    def execute(self, context):
        try:
            self._validate_search()
            if self.filter_expression:
                self._add_filters()

            script_id = Variable.get('netsuite_script_id')
            account_number = Variable.get('netsuite_account_number')
            
            url = f'https://{account_number}.restlets.api.netsuite.com/app/site/hosting/restlet.nl?script={script_id}'

            oauth = OAuth1Session(
                client_key=Variable.get('netsuite_consumer_api_key'),
                client_secret=Variable.get('netsuite_consumer_secret'),
                resource_owner_key=Variable.get('netsuite_token_id'),
                resource_owner_secret=Variable.get('netsuite_token_secret'),
                realm=account_number,
                signature_method='HMAC-SHA256',
            )

            headers = { 'Content-Type': 'application/json' }

            response = oauth.get(
                url,
                headers=headers
            )
            
            response.raise_for_status()
            data = response.text
            
            self.logger.info(data)
            return data

        except Exception as err:
            print(err)
            raise err