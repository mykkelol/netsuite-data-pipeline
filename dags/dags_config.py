class Config:
    POSTGRES_CONFIG = {
        "host": "host.docker.internal",
        "port": "5432",
        "db": 0
    }

    POSTGRES_CONN_ID = "postgres_localhost"

    TRANSACTION_TYPES = {
        "transactions": [
            "customsearch_ms_gl_posting_transactions_india",
        ]
    }

    VALIDATOR_CONFIG = {
        "description_length": 10,
        "languages": [
            "en", "pl", "es", "de"
        ]
    }