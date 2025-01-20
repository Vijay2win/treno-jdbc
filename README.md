# JDBC Connector

This repository is an experimental project aimed at using JDBC to connect to Databricks, rather than directly reading files from S3. By avoiding direct file reads, we can prevent compute operations from being unnecessarily offloaded to Trino. Instead, Trino serves as a proxy, delegating most predicate and query processing to the target database.

---
## Key Objectives

### Efficient Query Processing
The project seeks to generalize and push down the majority of predicates and processing to the target database. The goal is not to perform distributed queries, but to leverage Trino as a SQL proxy for operations between datastores. That said, we’re open to distributed queries when it involves interaction between databases.

### OAuth Passthrough
We aim to create an open-source implementation of OAuth Passthrough, supporting app-to-app OAuth with user impersonation. In this model, the proxy uses user credentials to act on behalf of the user. Future iterations will expand support for IAM roles and other authorization mechanisms.

## Implementation Highlights
The core logic for the JDBC connection resides in the `DatabricksConnectionFactory` (logic for OAuth pass-through) and `DatabricksClient` (this class translates to hive sql formated query). This serves as the foundation for establishing connections and managing query delegation.

---

## Usage

### Python Package
Install the necessary Python package using Conda:
```bash
conda install conda-forge::trino-python-client
```

Below is an example of how to connect to Databricks using the Trino connector:

```python
from trino.dbapi import connect
import pandas as pd

# Establish connection
conn = connect(
    host="localhost",
    port=8080,
    user="test",
    catalog="databricks",
    schema="databricks",
    extra_credential=[('oauth-token', 'sample_token')]
)

# Execute a query and fetch results
df = pd.read_sql_query("SELECT * FROM tables_test", conn)
print(df)
```

---

## Additional Tools

### Maven Dependency Sorting
To sort Maven dependencies by `groupId` and `artifactId`, use the following command:
```bash
mvn com.github.ekryd.sortpom:sortpom-maven-plugin:sort -Dsort.sortDependencies=groupId,artifactId
```

---

## Installation

Download the JDBC client from xxx to src/main/resources/jdbc/driver

```bash
mvn clean package -DskipTests 
```

Copy the packages to trino plugins folder and restart trino and configure the connector

---

## Future Work
- Expand support for IAM roles to improve access control and flexibility.
- Enhance OAuth integration to support broader use cases and simplify app-to-app communication.
- Explore distributed query support for complex interactions between multiple datastores.

We welcome contributions and suggestions to improve this project. Please feel free to submit issues or pull requests!

