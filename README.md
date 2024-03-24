[![Build Status](https://travis-ci.com/mykkelol/netsuite-data-pipeline.svg?&branch=main)](https://travis-ci.org/mykkelol/netsuite-data-pipeline) [![Coverage Status](https://coveralls.io/repos/github/mykkelol/netsuite-data-pipeline/badge.svg?branch=main)](https://coveralls.io/github/mykkelol/netsuite-data-pipeline?branch=main) [![Python 3.9](https://img.shields.io/badge/python-3.9-blue.svg)](https://www.python.org/downloads/release/python-390/)

# Finance Data Pipeline

**Finance Data Pipeline** is a purpose-built automated data pipeline to help Finance and Accounting teams adhere to evolving accounting standards, compliance, and regulations without additional manual intervention. The pipeline leverages parallel programming, REST APIs, and incremental load ELT architecture to support the following stack:

- **ERP**: [NetSuite](https://www.netsuite.com/)
- **ERP API**: [SuiteScript](https://docs.oracle.com/en/cloud/saas/netsuite/ns-online-help/section_4387799403.html#SuiteScript-2.x-RESTlet-Script-Type)
- **Orchestrator**: [Airflow](https://airflow.apache.org/)
- **Database**: [Postgres](https://www.postgresql.org/)
- **Datalake**: [AWS S3](https://aws.amazon.com/s3/)
- **Analytics**: [Tableau](https://help.tableau.com/current/pro/desktop/en-us/examples_postgresql.htm), [Mode](https://mode.com/integrations/postgresql), [Adaptive Planning](https://hightouch.com/integrations/postgresql-to-workday-adaptive-planning)
- **Internal Tool**: [Retool](https://retool.com/)

# Problem

In 2023, India Ministry of Corporate Affairs (MCA) issued a mandate obligating daily backups of all financial data and related documents (:point_right: see [MCA Notification 21st August 2022](https://resource.cdn.icai.org/71244clcgc160822.pdf)). In short, subsidiaries or legal entities operating in India—including those headquartered in the US but with branches in India—are obligated to maintain daily backups of financial data on servers physically located within India.

# Architecture

![Architecture](./images/architecture.png)

# How it works

The Finance Data Pipeline facilitates Finance and Accounting teams' compliance requirements without increasing manual work. It leverages Airflow, parallel programming, and an incremental load ELT architecture to automatically:

1. **Extract data from NetSuite ERP** by executing a pool of DAG tasks hourly to call a custom NetSuite SuiteScript API to extract transactions and related metadata such as vendors, customers, employees, departments, files, etc. asynchronously and dynamically.
2. **Load data** into SQL warehouse and India-hosted S3 lake
3. **Transform data** to structured, auditable, and reportable data, empowering Finance and Accounting teams to consume the structured data directly in analytics tools like Mode, Tableau, Adaptive, and Pigment, as well as custom reconciliation tools such as Retool and Superblocks.

Data engineers interested in this project must have knowledge of NetSuite, SuiteScript, Accounting, and REST APIs to effectively operate the custom Airflow's NetSuite operators.

# Running locally

# License

- You are free to fork and use this code directly according to the Apache License (:point_right: see [LICENSE](./LICENSE)).
- Please do not copy it directly.
- Crediting the author is appreciated.

# Contact
