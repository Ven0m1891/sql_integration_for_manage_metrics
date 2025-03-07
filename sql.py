import json
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, ForeignKey, MetaData, UniqueConstraint, Index, inspect
from sqlalchemy.orm import sessionmaker
import pandas as pd


def load_config():
    '''Загрузка конфигурации'''
    with open('config_sql.json', 'r', encoding='utf-8') as f:
        return json.load(f)


def get_database_url(config, project_name):
    '''Получение конфигурации для подключения к базе данных'''

    db_config = config['connect']
    db_name = config["sql"][project_name]["main_table"]
    database_url = f"postgresql://{db_config['LOGIN']}:{db_config['PASSWORD']}@{db_config['SERVER']}:{db_config['PORT']}/{db_name}"
    return database_url


def get_tables(config, project_name):
    """
    Получает имена таблиц для конкретного проекта из конфигурации.
    Возвращает имена таблиц: sku_table, metrics_online_table.
    """
    sql_config = config['sql'].get(project_name)
    if sql_config:
        return (
            sql_config['sku_table'],
            sql_config['metrics_online_table']
        )
    else:
        raise ValueError(f"Конфигурация для {project_name} не найдена")


def initialize_tables(project_name, config):
    """ Инициализирует таблицы для проекта, если они не существуют."""
    
    sku_table_name, metrics_online_table_name = get_tables(config, project_name)  # Получаем имена таблиц
    
    metadata = MetaData()

    # Определение таблиц
    sku_table = Table(
        sku_table_name, metadata,
        Column('id', Integer, primary_key=True),
        Column('sku_code', String, unique=True, nullable=False)
    )

    metrics_online_table = Table(
        metrics_online_table_name, metadata,
        Column('id', Integer, primary_key=True),
        Column('sku_id', Integer, ForeignKey(f'{sku_table_name}.id'), nullable=False),
        Column('metric_name', String, nullable=False),
        Column('value', Float, nullable=False),
        UniqueConstraint('sku_id', 'metric_name', name='_sku_metric_uc'),
        Index('ix_sku_metric', 'sku_id', 'metric_name')
    )

    # Подключение к базе данных
    engine = create_engine(get_database_url(config, project_name), echo=True)
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()

    # Создаем таблицы, если их нет
    tables_to_create = {
        sku_table_name: sku_table,
        metrics_online_table_name: metrics_online_table
    }

    for table_name, table in tables_to_create.items():
        if table_name not in existing_tables:
            metadata.create_all(engine, tables=[table]) 

    return sku_table, metrics_online_table 


def get_session(database_url):
    """Создает и возвращает сессию для работы с базой данных."""
    engine = create_engine(database_url, echo=True)
    Session = sessionmaker(bind=engine)
    return Session()


def add_online_metrics_from_dict(metrics_data, project_config):
    """
    Добавляет онлайн метрики из словаря в базу данных.
    Сначала преобразует словарь в DataFrame, затем добавляет метрики в таблицу metrics_online.
    """
    # Загружаем конфигурацию
    project_name = project_config["project"]
    config = load_config()
    
    # Получаем URL подключения
    database_url = get_database_url(config, project_name)
    
    # Инициализируем таблицы для выбранного проекта
    sku_table, metrics_online_table = initialize_tables(project_name, config)
    
    # Получаем сессию
    session = get_session(database_url)

    try:
        # Преобразуем данные в DataFrame для удобства обработки
        rows = []
        for sku_code, metrics in metrics_data.items():
            for metric_name, value in metrics.items():
                rows.append({
                    'sku_code': sku_code,
                    'metric_name': metric_name,
                    'value': value
                })

        metrics_df = pd.DataFrame(rows)

        for _, row in metrics_df.iterrows():
            # Проверка, существует ли SKU
            sku_query = session.query(sku_table).filter(sku_table.c.sku_code == row['sku_code']).first()
            if not sku_query:
                # Если SKU не существует, создаем новый
                ins = sku_table.insert().values(sku_code=row['sku_code'])
                result = session.execute(ins)
                sku_id = result.inserted_primary_key[0]
            else:
                sku_id = sku_query.id

            # Проверяем, существует ли уже такая метрика
            metric_query = session.query(metrics_online_table).filter(
                metrics_online_table.c.sku_id == sku_id,
                metrics_online_table.c.metric_name == row['metric_name']
            ).first()

            if metric_query:
                # Обновляем метрику
                upd = metrics_online_table.update().where(
                    metrics_online_table.c.id == metric_query.id
                ).values(value=row['value'])
                session.execute(upd)
            else:
                # Добавляем новую метрику
                ins = metrics_online_table.insert().values(
                    sku_id=sku_id,
                    metric_name=row['metric_name'],
                    value=row['value']
                )
                session.execute(ins)

        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Ошибка при добавлении данных: {e}")
    finally:
        session.close()
        print(f"Запись онлайн метрик {project_name} в базу данных успешно завершена")



def get_online_metrics(project_config, sku_codes=None, metric_names=None):
    """
    Функция для извлечения онлайн-метрик для всех SKU или конкретных SKU по конкретной метрике или набору метрик.
    
    :param project_config: Конфигурация проекта, содержащая имя проекта и другие параметры
    :param sku_codes: Список кодов SKU для фильтрации (по умолчанию None — для всех SKU)
    :param metric_names: Список названий метрик для фильтрации (по умолчанию None — без фильтрации по метрикам)
    :return: Словарь, где ключ — код SKU, а значение — словарь метрик и их значений
    """
    # Загружаем конфигурацию
    project_name = project_config["project"]
    config = load_config()
    
    # Получаем URL подключения
    database_url = get_database_url(config, project_name)
    
    # Инициализируем таблицы для выбранного проекта
    sku_table, metrics_online_table = initialize_tables(project_name, config)
    
    # Получаем сессию
    session = get_session(database_url)

    try:
        # Формируем запрос с соединением таблиц
        query = session.query(
            metrics_online_table.c.metric_name, 
            metrics_online_table.c.value, 
            sku_table.c.sku_code
        ).join(sku_table, metrics_online_table.c.sku_id == sku_table.c.id)

        # Фильтрация по SKU, если sku_codes переданы
        if sku_codes:
            query = query.filter(sku_table.c.sku_code.in_(sku_codes))

        # Фильтрация по метрикам, если metric_names переданы
        if metric_names:
            query = query.filter(metrics_online_table.c.metric_name.in_(metric_names))

        # Извлекаем результаты
        metrics_list = query.all()

        # Преобразуем метрики в структуру: {sku_code: {metric_name: value, ...}, ...}
        metrics_dict = {}
        for metric_name, value, sku_code in metrics_list:
            if sku_code not in metrics_dict:
                metrics_dict[sku_code] = {}

            metrics_dict[sku_code][metric_name] = value

        return metrics_dict

    except Exception as e:
        print(f"Ошибка при извлечении онлайн-метрик {project_name}: {e}")
        return {}

    finally:
        session.close()