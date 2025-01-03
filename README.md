
The main logic is in DatabricksConnectionFactory.

### Install Python Package:
```bash
conda install conda-forge::trino-python-client
```

Code to access via connector.

```python
from trino.dbapi import connect
import pandas as pd

conn = connect(host="localhost", port=8080, user="test", catalog="databricks", schema="databricks", extra_credential=[('oauth-token', 'sample_token')])
df = pd.read_sql_query("SELECT * FROM tables_test", conn)
print(df)
```


Others:
Maven sort dependencies:
```cmd
mvn com.github.ekryd.sortpom:sortpom-maven-plugin:sort -Dsort.sortDependencies=groupId,artifactId
```