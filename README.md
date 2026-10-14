# 🚖 Real-time Data Lakehouse Platform: NYC Taxi Analytics

Полнофункциональная платформа данных (**Data Lakehouse**) на базе архитектуры **Medallion (Bronze ➔ Silver ➔ Gold)**. 

Проект сочетает в себе **Streaming Ingestion** (эмуляция потоковых данных с помощью Apache Kafka, Avro и Confluent Schema Registry), открытый формат таблиц **Apache Iceberg**, аналитический движок **Trino**, трансформации **dbt** и оркестрацию процессов в **Apache Airflow 3**.

В основе стека лежит использование формата **Apache Iceberg**, который развернут поверх S3-совместимого хранилища и управляется через REST Catalog. Расчеты производятся с помощью распределенного движка **Apache Spark**, трансформация и контроль качества — через **dbt**, а координация всех процессов автоматизирована в **Apache Airflow**.

---

## 🏗️ Архитектура и Движение Данных

```
 [ NYC Gov Data ] ➔ [ PyArrow Producer ] ➔ [ Apache Kafka ] (Avro / Schema Registry)
                                                    │
                                         Spark Structured Streaming
                                                    │
                                                    ▼
                     ┌──────────────────────────────────────────────┐
                     │           S3 / MinIO Object Storage          │
                     │          (Managed by Iceberg REST Catalog)   │
                     ├──────────────────────────────────────────────┤
                     │  BRONZE LAYER : Raw Streaming & CSV Data     │
                     └──────────────────────┬───────────────────────┘
                                            │
                                    dbt (on Spark Connect)
                                            │
                     ┌──────────────────────▼───────────────────────┐
                     │  SILVER LAYER : Cleaned, Validated & Contracted
                     └──────────────────────┬───────────────────────┘
                                            │
                                    dbt (on Spark Connect)
                                            │
                     ┌──────────────────────▼───────────────────────┐
                     │  GOLD LAYER   : Aggregated Hourly Analytics  │
                     └──────────────────────┬───────────────────────┘
                                            │
                                     [ Trino Query Engine ]
```

---

## 📊 Слои данных (Medallion Architecture)

1. **Bronze (`icbrg.bronze`)**:
   * `yellow_tripdata`: Потоковые данные поездок, вычитываемые из Kafka-топика `yellow_tripdata_raw` через `Spark Structured Streaming`. Схема парсится на лету из Confluent Schema Registry (`from_avro`). Данные партиционируются по месяцам (`months(kafka_ts)`).
   * `taxi_zone`: Справочник районов Нью-Йорка, напрямую загружаемый из HTTP-источника на S3 (MinIO).
2. **Silver (`icbrg.silver`)**:
   * `silver_taxi_trips` & `silver_taxi_zones`: Очищенные данные с применением **Data Contracts** (`contract: enforced: true`), бизнес-валидацией (диапазоны пассажиров, отрицательные суммы) и физической оптимизацией хранения в Iceberg (`write.sort.mode: hierarchical`).
3. **Gold (`icbrg.gold`)**:
   * `gold_hourly_zone_metrics`: Бизнес-витрина, рассчитывающая агрегированные показатели (выручка, средний чек, % чаевых, дистанция) по районам и часам для подсистем BI и Trino.

---

## 🛠️ Стек технологий

* **Оркестрация:** Apache Airflow 3.3.0 + Astronomer Cosmos (dbt integration)
* **Streaming & Event Ingestion:** Apache Kafka 4.4 + Confluent Schema Registry + Kafka UI
* **Вычисления и Процессинг:** Apache Spark 4.1.3 (Master/Worker Cluster + Spark Connect Server)
* **Формат хранения:** Apache Iceberg 1.11.0 (REST Catalog)
* **Трансформация и Data Quality:** dbt-core + dbt-spark (Data Contracts & `dbt_utils`)
* **Ad-hoc SQL Queries:** Trino 483
* **Объектное хранилище (S3) & БД:** MinIO, PostgreSQL 16
* **Инфраструктура:** Docker & Docker Compose

---

## 🚀 Быстрый запуск

### 1. Требования
* **Docker** & **Docker Compose**.
* Выделите Docker-контейнерам минимум **6 CPU** и **8 GB RAM** (из-за одновременно работающих Spark Cluster, Trino и Kafka).

### 2. Важное примечание по переменным окружения (`.env`)
> ⚠️ **Безопасность на проде:** В данном репозитории все `.env` файлы в папке `./env/` намеренно **оставлены открытыми для удобства локального развертывания и проверки** проекта. В реальных условиях эксплуатации и продакшн-средах эти файлы были бы занесены в `.gitignore`, а вместо них использовались бы только безопасные шаблоны `.env.template`.

### 3. Подъем инфраструктуры
```bash
docker compose up --build -d
```

При старте автоматические скрипты:
* Инициализируют системные базы данных в PostgreSQL (`init_dbs.sh`).
* Создадут S3-бакет `warehouse` в MinIO (`init_minio.sh`).
* Подтянут необходимый набор JAR-библиотек (AWS SDK, Iceberg Runtime, Spark-Kafka-Avro) в контейнеры Spark.
* Установят dbt-зависимости (`dbt_utils`) внутри окружения Airflow.

---

## 🔌 Веб-интерфейсы

| Сервис | URL | Логин / Пароль |
| :--- | :--- | :--- |
| **Airflow UI** | `http://localhost:18080` | `admin` / `password` |
| **Kafka UI** | `http://localhost:8089` | — |
| **MinIO Console** | `http://localhost:9001` | `admin` / `password` |
| **Spark Master UI** | `http://localhost:8088` | — |
| **Trino HTTP** | `http://localhost:28080` | — |

---

## ⚙️ Настройка Airflow Connections

В веб-интерфейсе Airflow (`Admin -> Connections`) должны быть настроены следующие соединения:

1. **`nyc-gov`**: Type: `HTTP`, Host: `https://cloudfront.net`
2. **`spark-local`**: Type: `Spark Connect`, Host: `spark-connect`, Port: `15002`
3. **`kafka`**: Type: `Kafka`, Extra: `{"bootstrap.servers": "kafka:9092"}`
4. **`avro_reg`**: Type: `HTTP`, Host: `http://schema-registry:8081`

---

## 🔄 Запуск Пайплайнов

1. Запустите DAG **`nyc_tlc_kafka_provider`** — он начнет эмулировать поток сырых Parquet-данных в Kafka с Avro-сериализацией.
2. Запустите DAG **`nyc_tlc`** — он в потоковом режиме вычитает события из Kafka, запишет их в Iceberg Bronze, проведет dbt-трансформации Silver/Gold и сделает витрины доступными для Trino.