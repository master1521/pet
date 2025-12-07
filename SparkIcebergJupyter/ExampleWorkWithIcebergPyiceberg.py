"""
Модуль для работы с Apache Iceberg через PyIceberg.
"""
# from datetime import datetime, timedelta
import logging
# import json
from pprint import pp
import pandas as pd
import pyarrow as pa
from typing import Optional, Dict
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
# from pyiceberg.expressions import GreaterThan, LessThan, And, EqualTo, In, IsNull
# from pyiceberg.types import (
#     NestedField,
#     LongType,
#     StringType,
#     TimestampType,
#     DoubleType
# )

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def connect_to_catalog() -> object:
    """ Подключиться к Iceberg REST Catalog.
     Returns: Объект каталога для работы с таблицами
    """
    catalog = load_catalog(
        "iceberg",
        **{
            "type": "rest",
            "uri": "http://localhost:8181",
            "warehouse": "s3://warehouse/",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "s3.region": "us-east-1",
            "s3.path-style-access": "true"
        }
    )
    logger.info("Подключение к каталогу установлено")
    return catalog


def create_namespace(catalog: object, namespace: str) -> None:
    """ Создать namespace если не существует
    Args:
        catalog: Объект каталога
        namespace: Имя namespace
    """
    try:
        catalog.create_namespace(namespace)
        logger.info(f"Namespace '{namespace}' создан")
    except Exception:
        logger.info(f"Namespace '{namespace}' уже существует")


def convert_dataframe_to_arrow(df: pd.DataFrame) -> pa.Table:
    """ Конвертировать Pandas DataFrame в PyArrow Table с правильными типами.
    Iceberg требует timestamp в микросекундах (us), а не наносекундах (ns).
    Args: df: Pandas DataFrame
    Returns: PyArrow Table совместимая с Iceberg
    """
    # Конвертировать timestamp колонки в микросекунды
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype('datetime64[us]')

    arrow_table = pa.Table.from_pandas(df)
    logger.info(f"Схема PyArrow:\n{arrow_table.schema}")
    return arrow_table


def create_table(catalog: object, identifier: str, schema: Schema) -> object:
    """ Создать таблицу в каталоге или загрузить существующую.
    Args:
        catalog: Объект каталога
        identifier: Полный идентификатор таблицы (например, "dev.users")
        schema: Схема таблицы
    Returns: Объект таблицы (созданной или существующей)
    """
    from pyiceberg.exceptions import TableAlreadyExistsError

    try:
        table = catalog.create_table(identifier=identifier, schema=schema)
        logger.info(f"✅ Таблица создана: {table.location()}")
        logger.info(f"📊 Схема таблицы:\n{table.schema()}")
    except TableAlreadyExistsError:
        logger.info(f"📂 Таблица '{identifier}' уже существует, загружаем...")
        table = catalog.load_table(identifier)
        logger.info(f"📍 Location: {table.location()}")
    return table


def write_data_to_table(table: object, arrow_table: pa.Table) -> int:
    """Записать PyArrow данные в таблицу Iceberg.
    Args:
        table: Объект таблицы Iceberg
        arrow_table: PyArrow таблица с данными
    Returns: int - количество записанных строк
    Example:
        df = prepare_sample_data(n=1000)
        arrow_data = convert_dataframe_to_arrow(df)
        written = write_data_to_table(table, arrow_data)
    """
    rows_count = len(arrow_table)

    if rows_count == 0:
        logger.warning("Нет данных для записи")
        return 0

    try:
        table.append(arrow_table)
        logger.info(f"Записано строк: {rows_count:,}")
        return rows_count
    except Exception as e:
        logger.error(f"Ошибка при записи: {e}")
        logger.error(f"Колонки в данных: {arrow_table.column_names}")
        raise


def write_data_in_batches(
        table: object,
        arrow_table: pa.Table,
        batch_size: int = 10000,
        show_progress: bool = True
) -> int:
    """ Записать большой PyArrow Table батчами.
    Args:
        table: Объект таблицы Iceberg
        arrow_table: PyArrow таблица с данными
        batch_size: Размер батча (количество строк)
        show_progress: Показывать прогресс записи
    Returns: int - общее количество записанных строк
    Example:
        # Большой Arrow Table
        df = pd.read_csv("big_data.csv")
        arrow_data = convert_dataframe_to_arrow(df)

        # Записать батчами по 50k строк
        total = write_data_in_batches(table, arrow_data, batch_size=50000)
        print(f"Записано: {total:,} строк")
    """
    total_rows = len(arrow_table)

    if total_rows == 0:
        logger.warning("Нет данных для записи")
        return 0

    # Посчитать количество батчей
    num_batches = (total_rows + batch_size - 1) // batch_size

    if show_progress:
        logger.info(f"БАТЧ-ЗАПИСЬ:")
        logger.info(f"Всего строк: {total_rows:,}")
        logger.info(f"Размер батча: {batch_size:,}")
        logger.info(f"Количество батчей: {num_batches}")
    written_total = 0

    # Записать каждый батч
    for i in range(0, total_rows, batch_size):
        batch_num = (i // batch_size) + 1
        batch = arrow_table.slice(i, min(batch_size, total_rows - i))

        if show_progress:
            logger.info(f"Батч {batch_num}/{num_batches}: {len(batch):,} строк")

        # Записать батч
        written = write_data_to_table(table, batch)
        written_total += written

    if show_progress:
        logger.info(f"БАТЧ-ЗАПИСЬ ЗАВЕРШЕНА")
        logger.info(f"Всего записано: {written_total:,} строк")
    return written_total


def add_column_to_table(
        table: object,
        column_name: str,
        column_type: object,
        required: bool = False,
        doc: str = ""
) -> None:
    """ Добавить новую колонку в таблицу Iceberg.
    ⚠️ОГРАНИЧЕНИЕ ICEBERG:
    Required колонки можно добавить только в ПУСТУЮ таблицу.
    Для таблиц с данными используйте required=False (optional колонки).
    Причина: старые строки не будут иметь значений для новой required колонки,
    что нарушит целостность данных.
    Args:
        table: Объект таблицы Iceberg
        column_name: Имя новой колонки
        column_type: Тип данных (LongType, StringType, DoubleType и т.д.)
        required: Обязательная колонка (работает только для пустых таблиц)
        doc: Описание колонки
    Example:
        from pyiceberg.types import StringType, LongType, DoubleType

        # Добавить optional колонку
        add_column_to_table(table, "phone", StringType())

        # С описанием
        add_column_to_table(
            table,
            "age",
            LongType(),
            doc="Возраст пользователя"
        )

        # Ошибка: required в таблице с данными
        add_column_to_table(table, "age", LongType(), required=True)
    """
    # Проверка ПЕРЕД попыткой добавления
    if required:
        rows_count = len(table.scan().to_arrow())
        if rows_count > 0:
            logger.error(f"❌ ОГРАНИЧЕНИЕ ICEBERG:")
            logger.error(f"   Нельзя добавить required колонку в таблицу с данными")
            logger.error(f"   В таблице: {rows_count:,} строк")
            logger.error(f"   💡 Используйте required=False для optional колонки")
            raise ValueError(
                f"Нельзя добавить required колонку '{column_name}' в таблицу с {rows_count} строками. "
                f"Используйте required=False"
            )

    try:
        with table.update_schema() as update:
            update.add_column(
                path=column_name,
                field_type=column_type,
                required=required,
                doc=doc
            )
        logger.info(f"Колонка '{column_name}' добавлена")
        logger.info(f"Тип: {column_type}")
        logger.info(f"Обязательная: {required}")

        if doc:
            logger.info(f"   Описание: {doc}")
    except Exception as e:
        logger.error(f"Ошибка при добавлении колонки '{column_name}': {e}")
        raise


def update_rows_by_condition(
        table: object,
        update_column: str,
        new_value: any,
        filter_condition: object,
        show_preview: bool = True
) -> int:
    """ Обновить данные в таблице Iceberg по условию.
    ⚠️ МЕХАНИЗМ ICEBERG:
    1. Прочитать все данные
    2. Изменить нужные строки в памяти
    3. Удалить старые строки (DELETE)
    4. Записать обновленные строки (INSERT)
    Это Copy-On-Write паттерн - физически создаются новые файлы.
    Args:
        table: Объект таблицы Iceberg
        update_column: Колонка для обновления
        new_value: Новое значение
        filter_condition: Условие фильтрации (EqualTo, GreaterThan и т.д.)
        show_preview: Показать строки до и после обновления
    Returns: int - количество обновленных строк

    Example:
        from pyiceberg.expressions import GreaterThan, EqualTo

        # Обновить age = 99 где balance > 1000
        updated = update_rows_by_condition(
            table,
            update_column="age",
            new_value=99,
            filter_condition=GreaterThan("balance", 1000)
        )

        # Обновить user_name = "Admin" где id = 5
        update_rows_by_condition(
            table,
            update_column="user_name",
            new_value="Admin",
            filter_condition=EqualTo("id", 5)
        )
    """
    # Найти строки для обновления
    logger.info(f"Поиск строк для обновления...")
    matching_data = table.scan().filter(filter_condition).to_pandas()
    rows_count = len(matching_data)

    if rows_count == 0:
        logger.info("Нет строк соответствующих условию")
        return 0

    logger.info(f"Найдено строк: {rows_count:,}")

    # Показать preview ДО обновления
    if show_preview and rows_count > 0:
        logger.info(f"СТРОКИ ДО ОБНОВЛЕНИЯ (первые 5):")
        preview_before = matching_data.head(5)[
            [update_column] + [col for col in matching_data.columns if col != update_column][:3]]
        logger.info(f"{preview_before.to_string(index=False)}\n")

    # Обновить значения в памяти
    logger.info(f"Обновление колонки '{update_column}' = {new_value}...")
    matching_data[update_column] = new_value

    # Показать preview ПОСЛЕ обновления
    if show_preview and rows_count > 0:
        logger.info(f"СТРОКИ ПОСЛЕ ОБНОВЛЕНИЯ (первые 5):")
        preview_after = matching_data.head(5)[
            [update_column] + [col for col in matching_data.columns if col != update_column][:3]]
        logger.info(f"\n{preview_after.to_string(index=False)}\n")
    logger.warning(f"ОБНОВЛЕНИЕ {rows_count:,} строк (DELETE + INSERT)")

    try:
        # Удалить старые строки
        logger.info(f"1/2: Удаление старых строк...")
        table.delete(filter_condition)

        # Записать обновленные строки
        logger.info(f"2/2: Запись обновленных строк...")
        arrow_data = pa.Table.from_pandas(matching_data)
        table.append(arrow_data)

        logger.info(f"ОБНОВЛЕНИЕ ЗАВЕРШЕНО:")
        logger.info(f"Обновлено строк: {rows_count:,}")
        logger.info(f"Колонка: {update_column}")
        logger.info(f"Новое значение: {new_value}")
        return rows_count
    except Exception as e:
        logger.error(f"Ошибка при обновлении: {e}")
        raise


def read_table_data_batched(
        table: object,
        row_filter: Optional[object] = None,
        selected_fields: Optional[tuple] = None,
        limit: Optional[int] = None,
        batch_size: int = 10000
) -> pd.DataFrame:
    """ Эффективное чтение данных батчами для больших таблиц.
    Args:
        table: Объект таблицы Iceberg
        row_filter: Фильтр для строк
        selected_fields: Кортеж колонок для выборки
        limit: Ограничение количества строк
        batch_size: Размер батча для чтения
    Returns:
        Pandas DataFrame с данными
    """
    scan = table.scan()
    if row_filter is not None:
        scan = scan.filter(row_filter)

    if selected_fields is not None:
        scan = scan.select(*selected_fields)

    arrow_table = scan.to_arrow()
    if limit is not None and len(arrow_table) > limit:
        arrow_table = arrow_table.slice(0, limit)

    df = arrow_table.to_pandas()
    table_name = table.name()
    logger.info(f"Прочитано строк из таблицы '{table_name}': {len(df)}")
    return df


def rename_column_to_hide(table: object, column_name: str) -> None:
    """ Скрыть колонку через переименование.
    ВАЖНО: В Iceberg нельзя физически удалить колонку из схемы.
    Эта функция переименовывает колонку чтобы она не использовалась.
    Args:
        table: Объект таблицы
        column_name: Имя колонки для скрытия
    """
    hidden_name = f"_removed_{column_name}"

    with table.update_schema() as update:
        update.make_column_optional(column_name)
        update.rename_column(column_name, hidden_name)
    logger.info(f"Колонка скрыта: {column_name} -> {hidden_name}")


def drop_table_if_exists(catalog: object, table_identifier: str) -> None:
    """ Удалить таблицу если существует.
    Удаляет и файлы данных, и метаданные
    Args:
        catalog: Объект каталога
        table_identifier: Полный идентификатор таблицы (namespace.table)
    """
    try:
        catalog.purge_table(table_identifier)
        logger.info(f"Таблица '{table_identifier}' удалена")
    except Exception:
        logger.info(f"Таблица '{table_identifier}' не существовала")


def delete_rows(
        table: object,
        row_filter: object,
        dry_run: bool = False,
        show_preview: bool = False,
        preview_limit: int = 10
) -> int:
    """ Удалить строки из таблицы по условию фильтрации.
    ⚠️ Операция создает новый snapshot. Данные физически остаются до cleanup.
    Args:
        table: Объект таблицы Iceberg
        row_filter: Условие фильтрации (EqualTo, GreaterThan, In и т.д.)
        dry_run: True = только подсчитать, False = реально удалить
        show_preview: Показать строки которые будут удалены
        preview_limit: Сколько строк показать (при show_preview=True)
    Returns: int - количество удаленных строк
    Example:
        from pyiceberg.expressions import EqualTo, GreaterThan

        # Посмотреть какие строки будут удалены
        count = delete_rows(
            table,
            GreaterThan("balance", 500),
            dry_run=True,
            show_preview=True,  # Показать строки
            preview_limit=20    # Показать первые 20
        )

        # Если все ок - удалить
        delete_rows(table, GreaterThan("balance", 500))
    """
    # Подсчитать и получить строки для удаления
    matching_data = table.scan().filter(row_filter).to_pandas()
    rows_to_delete = len(matching_data)

    if rows_to_delete == 0:
        logger.info("Нет строк соответствующих фильтру")
        return 0

    # Показать preview если запросили
    if show_preview:
        logger.info(f"СТРОКИ ДЛЯ УДАЛЕНИЯ (всего: {rows_to_delete:,}):")
        logger.info(f"Показано первых {min(preview_limit, rows_to_delete)} строк:\n")

        preview_df = matching_data.head(preview_limit)
        logger.info(f"{preview_df.to_string(index=False)}")

        if rows_to_delete > preview_limit:
            logger.info(f"... и еще {rows_to_delete - preview_limit} строк")

    if dry_run:
        logger.info(f"Режим проверки (dry_run):")
        logger.info(f"Найдено строк для удаления: {rows_to_delete:,}")
        logger.info(f"Условие: {row_filter}")

        if not show_preview:
            logger.info(f"Добавьте show_preview=True чтобы увидеть строки")
        return rows_to_delete
    logger.warning(f"УДАЛЕНИЕ {rows_to_delete:} строк")

    try:
        table.delete(row_filter)
        logger.info(f"Удалено строк: {rows_to_delete:,}")
        remaining = len(table.scan().to_arrow())
        logger.info(f"Осталось строк: {remaining:,}")
        return rows_to_delete
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        raise

