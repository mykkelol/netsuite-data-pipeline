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
            'filter_expression': [
                ['taxline','is','F'], 
                'AND', 
                ['cogs','is','F'], 
                'AND', 
                ['posting','is','T'], 
                'AND', 
                ['accountingbook','anyof', ACCOUNTING_BOOKS['secondary']]
            ]
        }
    ]

    S3_CONFIG = {
        'host': 'host.docker.internal',
        'port': '5432',
        'db': 0
    }

    S3_CONN_ID = 'minio_conn'

    LANDING_BUCKET = 'finance-data-lake-landing'
    LAKE_BUCKET = 'finance-data-lake'

    POSTGRES_CONFIG = {
        'host': 'host.docker.internal',
        'port': '5432',
        'db': 'finance'
    }

    POSTGRES_CONN_ID = 'postgres_localhost'