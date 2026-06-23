# S3 to Vertica Data Warehouse Pipeline

## Overview

Проект реализует загрузку и обработку данных из S3 в аналитическое хранилище Vertica.

Пайплайн построен по архитектуре Data Vault и оркестрируется через Airflow.

---

## Data Sources

Файлы из S3:

- users.csv
- groups.csv
- dialogs.csv
- group_log.csv

---

## Pipeline (Airflow)

DAG выполняет два этапа:

### 1. Load from S3
- скачивание файлов из S3
- базовая проверка (print первых строк)

### 2. Load to STG
- загрузка данных в staging таблицы Vertica
- TRUNCATE + COPY загрузка

---

## Data Vault Model

### Hubs
- h_users
- h_groups

### Links
- l_user_group_activity  
(связь пользователь ↔ группа)

### Satellites
- s_auth_history  
(история событий: add, etc.)

---

## Business Logic

Построен аналитический запрос:

- топ групп по регистрации
- активность пользователей в группах
- конверсия добавлений → сообщений

---

## Tech Stack

- Apache Airflow
- Vertica
- AWS S3 (Yandex S3 compatible)
- Python
- SQL (Data Vault model)

---

## Engineering Decisions

- Использована Data Vault модель для масштабируемого DWH
- STG слой отделён от DWH логики
- загрузка через COPY (эффективно для Vertica)
- Airflow TaskFlow API + TaskGroup для структуры DAG
- разделение ingestion и loading этапов

---

## What I learned

- построение Data Vault модели (Hubs, Links, Satellites)
- работа с Vertica как аналитической БД
- загрузка данных из S3 в DWH
- оркестрация пайплайнов через Airflow
- проектирование ETL процессов в batch-архитектуре
