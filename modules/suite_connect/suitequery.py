import json
from requests_oauthlib import OAuth1Session
from airflow.models import Variable

from log import log

@log
class SuiteQuery:
    SIGNATURE_METHOD = 'HMAC-SHA256'
    HEADERS = { 'Content-Type': 'application/json' }

    def __init__(self, filters, columns):
        self.filters = filters
        self.columns = columns
        self._account_id = ''
        self._script_id = ''
        self._consumer_key = ''
        self._consumer_secret = ''
        self._token_key = ''
        self._token_secret = ''
        self._results = []

    def _get_url(self):
        url = (
            f'https://{self._account_id}'
            f'.restlets.api.netsuite.com'
            f'/app/site/hosting/restlet.nl'
            f'?script={self._script_id}'
        )

        if self.filters: 
            url += f'&filters={self.filters}'
        if self.columns: 
            url += f'&columns={self.columns}'

        return url

    def _set_variables(self):
        variable_keys = {
            '_account_id': 'netsuite_account_id',
            '_script_id': 'netsuite_script_id',
            '_consumer_key': 'netsuite_consumer_api_key',
            '_consumer_secret': 'netsuite_consumer_secret',
            '_token_key': 'netsuite_token_id',
            '_token_secret': 'netsuite_token_secret'
        }

        for variable_value, variable_key in variable_keys.items():
            setattr(self, variable_value, Variable.get(variable_key))

    def _set_connection(self):
        conn = OAuth1Session(
            client_key=self._consumer_key,
            client_secret=self._consumer_secret,
            resource_owner_key=self._token_key,
            resource_owner_secret=self._token_secret,
            realm=self._account_id,
            signature_method=SuiteQuery.SIGNATURE_METHOD,
        )
        
        return conn
    
    @staticmethod
    def valid_response(response):
        return (
            response.status_code == 200
            and response.text.strip()
        )
    
    def extract_results(self):
        try:
            self._set_variables()
            conn = self._set_connection()
            
            response = conn.get(
                self._get_url(),
                headers=SuiteQuery.HEADERS
            )
            response.raise_for_status()
            
            if (self.valid_response(response)):
                data = json.loads(response.text).get('results')
                return data
            
        except Exception as err:
            self.logger.info(f"Error occurred: {err}")