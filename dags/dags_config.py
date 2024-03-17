class Config:
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
        }
    ]

    ACCOUNTING_BOOKS = {
        'primary': 0,
        'secondary': 3
    }

    S3_CONFIG = {
        'host': 'host.docker.internal',
        'port': '5432',
        'db': 0
    }

    S3_CONN_ID = 'minio_conn'

    POSTGRES_CONFIG = {
        'host': 'host.docker.internal',
        'port': '5432',
        'db': 'finance'
    }

    POSTGRES_CONN_ID = 'postgres_localhost'

    LANDING_BUCKET = 'finance-data-lake-landing'
    LAKE_BUCKET = 'finance-data-lake'