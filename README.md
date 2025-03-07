# SQL-интеграция для управления метриками

Этот проект предоставляет инструменты для работы с базой данных PostgreSQL, позволяя управлять метриками для различных проектов. Основная функциональность включает создание таблиц, добавление и извлечение метрик, а также интеграцию с конфигурационным файлом для настройки подключения к базе данных.

## Описание

Проект состоит из двух основных файлов:

1. **sql.py** — основной скрипт, содержащий функции для работы с базой данных.
2. **config_sql.json** — конфигурационный файл, содержащий настройки подключения к базе данных и имена таблиц для каждого проекта.

### Основные функции

- **Инициализация таблиц**: Создание таблиц для хранения SKU и метрик, если они не существуют.
- **Добавление метрик**: Добавление метрик из словаря в базу данных.
- **Извлечение метрик**: Получение метрик для конкретных SKU или всех SKU, с возможностью фильтрации по названиям метрик.

## Установка и использование

1.Для работы необходимы библиотеки `sqlalchemy` и `pandas`. Установите их с помощью pip:

   ```bash
   pip install sqlalchemy pandas
   ```

2. Создайте конфигурационный файл `config_sql.json` в корневой директории проекта. 

3. Используйте функции из `sql.py` для работы с базой данных. Пример использования:

   ```python
   from sql import add_online_metrics_from_dict, get_online_metrics

   # Пример добавления метрик
   metrics_data = {
       "SKU001": {"metric1": 10.5, "metric2": 20.3},
       "SKU002": {"metric1": 15.0, "metric2": 25.0}
   }
   project_config = {"project": "ProjectA"}
   add_online_metrics_from_dict(metrics_data, project_config)

   # Пример извлечения метрик
   sku_codes = ["SKU001", "SKU002"]
   metric_names = ["metric1"]
   metrics = get_online_metrics(project_config, sku_codes, metric_names)
   print(metrics)
   ```

## Структура конфигурационного файла

Конфигурационный файл `config_sql.json` содержит два основных раздела:

1. **connect**: Настройки подключения к базе данных PostgreSQL.
   - `PORT`: Порт базы данных.
   - `SERVER`: Адрес сервера базы данных.
   - `LOGIN`: Логин для подключения.
   - `PASSWORD`: Пароль для подключения.

2. **sql**: Конфигурация для каждого проекта.
   - `main_table`: Основная таблица для проекта.
   - `sku_table`: Таблица для хранения SKU.
   - `metrics_online_table`: Таблица для хранения онлайн-метрик.

Пример структуры (для 4 проектов):

```json
{
  "connect": {
    "PORT": "5432",
    "SERVER": "localhost",
    "LOGIN": "postgres",
    "PASSWORD": "your_password"
  },
  "sql": {
    "ProjectA": {
      "main_table": "project_a_main",
      "sku_table": "project_a_sku",
      "metrics_online_table": "project_a_metrics_online"
    },
    "ProjectB": {
      "main_table": "project_b_main",
      "sku_table": "project_b_sku",
      "metrics_online_table": "project_b_metrics_online"
    },
    "ProjectC": {
      "main_table": "project_c_main",
      "sku_table": "project_c_sku",
      "metrics_online_table": "project_c_metrics_online"
    },
    "ProjectD": {
      "main_table": "project_d_main",
      "sku_table": "project_d_sku",
      "metrics_online_table": "project_d_metrics_online"
    }
  }
}
```

## Лицензия

Этот проект распространяется под лицензией MIT. Подробности см. в файле [LICENSE](LICENSE).
