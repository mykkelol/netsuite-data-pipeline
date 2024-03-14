class Config:
    POSTGRES_CONFIG = {
        'host': 'host.docker.internal',
        'port': '5432',
        'db': 'finance'
    }

    POSTGRES_CONN_ID = 'postgres_localhost'

    SUPPORTED_RECORD_TYPES = [
        'transaction',
        'customer',
        'class',
        'department',
        'subsidiary',
        'currency',
    ]

    TRANSACTION_TYPES = {
        'transaction': [
            'customsearch_gl_posting_transactions_india',
        ]
    }

    ACCOUNTING_BOOKS = {
        'primary': 0,
        'secondary': 3
    }

    VALIDATOR_CONFIG = {
        'description_length': 10,
        'languages': [
            'en', 'pl', 'es', 'de'
        ]
    }

    S3_CONFIG = {
        "host": "host.docker.internal",
        "port": "5432",
        "db": 0
    }

    S3_CONN_ID = "minio_conn"

    LANDING_BUCKET = 'finance-data-lake-landing'
    LAKE_BUCKET = 'finance-data-lake'