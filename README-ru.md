# Running Spark Locally using Docker Compose

> [English](./README.md)

Основано на [karlchris/spark-docker](https://karlchris.github.io/data-engineering/projects/spark-docker/).

1. Найти гитхаб и скачать репозиторий.
2. Заменить файлы в нем на файлы из этого репозитория.
3. Запустить кластер
```bash
docker-compose up
```

4. Зайти в master-контейнер
```bash
docker exec -it spark-master /bin/bash
```

5. Запустить pyspark
```bash
# bash докер-контейнера master-ноды
pyspark
```

6. Вывалится ссылка. Перейти по ней, чтобы попасть в веб-версию Jupyter
```
http://127.0.0.1:8889/tree?token=...
```

Сервисы кластера теперь доступны локально в браузере:

- http://localhost:8080 - WebUI мастер-узла.
  - Должны быть видны и доступны worker'ы.
  - Текущий процесс jupyter должен быть представлен в виде активной сессии PySparkSession. Ему должны быть назначены workers и cores.
- http://localhost:18080 - history-сервер Spark.
  - История джобов.
  - Просмотр логов и статистики по ресурсам на каждый запуск "Application" ("SparkSession").
