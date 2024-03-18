class Config:
    ACCOUNTING_BOOKS = {
        'primary': 0,
        'secondary': 3
    }
    
    SUPPORTED_RECORD_TYPES = [
        'transaction',
        'invoice',
        'vendorbill',
        'journalentry',
        'creditmemo',
        'payment',
        'customerpayment',
        'customer',
        'vendor',
        'class',
        'department',
        'subsidiary',
        'currency',
    ]

    RECORD_TYPES = [
        {
            'type': 'transaction',
            'search_id': 'customsearch_gl_posting_transactions_india',
            'filter': [
                ['taxline','is','F'], 
                'AND', 
                ['cogs','is','F'], 
                'AND', 
                ['posting','is','T'], 
                'AND', 
                ['accountingbook','anyof', ACCOUNTING_BOOKS['secondary']]
            ],
            'subsearches': [
                ('transaction', 'customsearch_gl_posting_transactions_india', []),
                ('vendor', 'customsearch_vendor', []),
                ('customer', 'customsearch_customer', []),
                ('class', 'customsearch_class', []),
                ('department', 'customsearch_department', []),
                ('subsidiary', 'customsearch_subsidiary', []),
                ('currency', 'customsearch_currency', []),
            ]
        }
    ]

    S3_CONN_ID = 'minio_conn'
    S3_CONFIG = {
        'host': 'http://host.docker.internal',
        'port': '9000',
    }
    
    LANDING_BUCKET = 'finance-data-lake-landing'
    LAKE_BUCKET = 'finance-data-lake'

    POSTGRES_CONN_ID = 'postgres_localhost'
    POSTGRES_CONFIG = {
        'host': 'host.docker.internal',
        'port': '5432',
        'db': 'finance'
    }
