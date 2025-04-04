# Running Spark Locally using Docker Compose

> [Russian / На русском](./README-ru.md)

Based on [karl chris/spark-docker](https://karlchris.github.io/data-engineering/projects/spark-docker/).

1. Start the cluster
```bash
docker-compose up
```

2. Log in to the master node
```bash
docker exec -it spark-master /bin/bash
```

3. Launch PySpark
```bash
# shell of the pyspark master node in docker container
pyspark
```

4. The link will be shown. Click on it to get to the web version of Jupyter
```
http://127.0.0.1:8889/tree?token=...
```

Cluster services are now available locally in the host machine browser:
- http://localhost:8080 - WebUI of the master node.
    - Workers must be visible and accessible.
    - The current jupyter process should be represented as an active *PySparkSession*. It should be assigned workers and cores.
- http://localhost:18080 - history-node.
    - The history of jobs.
    - View logs and statistics on resources for each launch of the *Application* (*SparkSession*).
