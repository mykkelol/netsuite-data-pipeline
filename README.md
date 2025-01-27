# Finance Data Pipeline

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](https://travis-ci.org/mykkelol/netsuite-data-pipeline) [![Coverage Status](https://img.shields.io/badge/coverage-100%25-brightgreen.svg)](https://coveralls.io/github/mykkelol/netsuite-data-pipeline?branch=main) [![Python 3.9](https://img.shields.io/badge/python-3.9-blue.svg)](https://www.python.org/downloads/release/python-390/) [![SuiteScript 2.1](https://img.shields.io/badge/suitescript-2.1-blue.svg)](https://docs.oracle.com/en/cloud/saas/netsuite/ns-online-help/chapter_156042690639.html#SuiteScript-2.1) [![License](https://img.shields.io/badge/License-apache-fuchsia.svg)](./LICENSE)

**Finance Data Pipeline** is a purpose-built data pipeline to help Finance teams in complying with the 2023 mandate from the India Ministry of Corporate Affairs (👉 see [MCA Notification 21st August 2022](https://resource.cdn.icai.org/71244clcgc160822.pdf)) without increasing manual workload by automatically orchestrating large volume of financial data across multiple systems. 

The mandate requires all enterprises operating in India, including branches of US-based companies, to maintain daily backups of financial data. The data pipeline helps Finance teams adhere to evolving accounting standards and regulations without increasing manual workload by leveraging parallel programming, REST APIs, and incremental load ELT architecture to support the following stack:

- **ERP**: [NetSuite](https://www.netsuite.com/), [SuiteScript](https://docs.oracle.com/en/cloud/saas/netsuite/ns-online-help/section_4387799403.html#SuiteScript-2.x-RESTlet-Script-Type)
- **Orchestrator**: [Airflow](https://airflow.apache.org/)
- **Data storage**: [Postgres](https://www.postgresql.org/), [AWS S3](https://aws.amazon.com/s3/)
- **Analytics**: [Tableau](https://help.tableau.com/current/pro/desktop/en-us/examples_postgresql.htm), [Mode](https://mode.com/integrations/postgresql), [Adaptive Planning](https://hightouch.com/integrations/postgresql-to-workday-adaptive-planning)
- **Internal Tool**: [Retool](https://docs.retool.com/data-sources/quickstarts/database/postgresql)

# Architecture

![Architecture](./images/architecture.png)

# How it works

**Finance Data Pipeline** facilitates Finance teams' compliance requirements without increasing manual work. It leverages Airflow, parallel programming, and an incremental load ELT architecture to automatically:

1. **Extract data from NetSuite ERP** by executing a pool of DAG tasks hourly to call a custom NetSuite SuiteScript API to extract transactions and related metadata such as vendors, customers, employees, departments, files, etc. asynchronously and dynamically.
2. **Load data** into SQL warehouse and India-hosted S3 lake.
3. **Transform data** to structured, audit-ready, and reportable data, empowering Finance and Accounting teams to consume the Financial data directly in analytics tools like Mode, Tableau, Adaptive, and Pigment, as well as custom reconciliation tools such as Retool and Superblocks.

Data engineers interested in this project must have knowledge of NetSuite, SuiteScript 2.1+, Accounting, and REST APIs to effectively operate the custom Airflow's NetSuite operators. The asynchronous and dynamic processing capability is available for both the pool of tasks and the DAG level (👉 see [Step 4: Configure DAG](#step-4-configure-dag)).

# Running Locally

### Step 1: Create NetSuite API

Create a SuiteScript 2.1+ RESTlet script to deploy a custom NetSuite REST API, retrieve its `script_id`, and store in `.env` file similar to [.env.example](./.env.example) or in Airflow UI through `Admin`/`Variables` and `Admin`/`Connections`

```JavaScript
define(['N/search'], function (search) {
    return onRequest: (c) => {
        const { type, id, filters, columns } = c;
        return search.create({
            type
            id,
            filters,
            columns
        })
        .run()
        .getRange(start, end)
        .map(rows => rows.getValue())
        .reduce((rows, row) => {
            return rows;
        }, results: [])
    }
})
```

### Step 2: Build Airflow project

```bash
git clone https://github.com/mykkelol/netsuite-data-pipeline
cd netsuite-data-pipeline
python3 -m pip install docker
python3 -m pip install docker-compose
docker build -t extending-airflow:latest .
docker-compose up -d
```

### Step 3: Create tables in SQL severs

```sql
CREATE TABLE IF NOT EXISTS my_table_name (
    id VARCHAR(255),
    duedate DATE,
    trandate DATE,
    amount DECIMAL,
    tranid VARCHAR(255),
    entity VARCHAR(255),
    status VARCHAR(255),
    currency VARCHAR(255),
    department VARCHAR(255),
    record_type VARCHAR(255),
    requester_name VARCHAR(255),
    requester_email VARCHAR(255),
    nextapprover_name VARCHAR(255),
    nextapprover_email VARCHAR(255),
    transaction_number VARCHAR(255),
    PRIMARY KEY(id)
)
```

### Step 4: Configure DAG

In [dags_config.py](./dags/dags_config.py), `RECORD_TYPE`/`search_id` is required to run the DAG. In the DAG below, the highlighted tasks represents the base `transaction` query required in `RECORD_TYPE` and `search_id`.

![DAG Primary Search](./images/dag_primary_search.png)

Optionally, leverage parallel programming and Airflow Task Group to optimize and enrich the data pipeline by configuring the `subsearches` property to add sub queries to support the base query. To do so, add tuples of (`record_type`, `search_id`, `filters`) to configure the DAG to run multiple task instances automatically, dynamically and asychronously.

```python
{
    'type': 'transaction',
    'search_id': 'search_id_india',
    'subsearches': [
        ('transaction', 'customsearch_gl_posting_transactions_india', []),
        ('customer', 'customsearch_customer', []),
        ('currency', 'customsearch_currency', []),
        ('some_record_type', 'customsearch_id', []),
    ]
}
```

Lastly, the pipeline can be extended even further to execute multiple DAGs dynamically and asynchronously. To do so, add additional base query dictionaries to `RECORD_TYPE`. Consider the following example, which involves a common scenario of a multi-subsidiary enterprise that operates with 2 subsidiaries (US, IN), then the pipeline can be configured to execute two DAGs for two base queries (or 2 subsidiaries).

```python
[
    {
        'type': 'transaction',
        'search_id': 'search_id_usa',
        'subsearches': [
            ('transaction', 'customsearch_gl_posting_transactions_usa', []),
            ('some_record_type', 'customsearch_id', []),
        ]
    },
    {
        'type': 'transaction',
        'search_id': 'search_id_india',
        'subsearches': [
            ('transaction', 'customsearch_gl_posting_transactions_india', []),
            ('some_record_type', 'customsearch_id', []),
        ]
    }
]
```

# Who Uses Finance Data Pipeline

Finance Data Pipeline is used by finance systems engineers in the developer community:

![Developer Community](./images/logos.png)

# License

- You are free to fork and use this code directly according to the Apache License (👉 see [LICENSE](./LICENSE)).
- Please do not copy it directly.
- Crediting the author is appreciated.

# Contact

- Github [@mykkelol](https://github.com/mykkelol)
- LinkedIn [/msihavong](https://linkedin.com/in/msihavong)
