"""
Catalog API — операции с таблицами, namespaces и views через Iceberg REST.

API: /api/catalog/v1/{catalog_name}/...
Документация: https://raw.githubusercontent.com/apache/polaris/refs/tags/apache-polaris-1.4.0/spec/generated/bundled-polaris-catalog-service.yaml

Возможности:
  - Config: get_catalog_config
  - Namespaces: list, create, get, exists, update_properties, drop
  - Tables: list, create, get, commit, exists, register, rename, drop
  - Views: list, create, get, update, exists, rename, drop

Использование:
    from polaris_client.session import PolarisSession
    from polaris_client.catalog_api import list_namespaces
    session = PolarisSession(host=..., client_id=..., client_secret=...)
    result = list_namespaces(session, "bronze")
"""

import requests

from polaris_client.session import PolarisSession


# =============================================================================
# Config
# =============================================================================


def get_catalog_config(session: PolarisSession, catalog_name: str) -> dict:
    """Получить конфигурацию каталога (warehouse URI, overrides, defaults).
    Curl: curl -X GET "http://localhost:8181/api/catalog/v1/config?warehouse=bronze" -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
    Returns: dict: overrides, defaults, конфигурация warehouse
    Example: cfg = get_catalog_config(session, "bronze")
    """
    url = f"{session.host}/api/catalog/v1/config"
    response = requests.get(url, params={"warehouse": catalog_name}, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


# =============================================================================
# Namespaces
# =============================================================================


def list_namespaces(session: PolarisSession, catalog_name: str) -> list[str]:
    """Получить список namespaces в каталоге.
    Curl: curl -X GET http://localhost:8181/api/catalog/v1/bronze/namespaces -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
    Returns: list[str]: список имён namespaces (вида "ns" или "ns.sub")
    Example: namespaces = list_namespaces(session, "bronze")
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/namespaces"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    raw = response.json().get("namespaces", [])
    return [".".join(parts) for parts in raw]


def create_namespace(
    session: PolarisSession,
    catalog_name: str,
    namespace: str,
    properties: dict | None = None,
) -> dict:
    """Создать namespace в каталоге.
    Curl: curl -X POST http://localhost:8181/api/catalog/v1/bronze/namespaces -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"namespace":["raw"],"properties":{}}'
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace
        properties: дополнительные свойства (опционально)
    Returns: dict: созданный namespace
    Example: create_namespace(session, "bronze", "raw")
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/namespaces"
    body = {"namespace": [namespace], "properties": properties or {}}
    response = requests.post(url, json=body, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def get_namespace(session: PolarisSession, catalog_name: str, namespace: str) -> dict:
    """Получить свойства namespace.
    Curl: curl -X GET http://localhost:8181/api/catalog/v1/bronze/namespaces/raw -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace
    Returns: dict: namespace и его properties
    Example: ns = get_namespace(session, "bronze", "raw")
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/namespaces/{namespace}"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def namespace_exists(session: PolarisSession, catalog_name: str, namespace: str) -> bool:
    """Проверить существование namespace через GET с обработкой 404.
    Curl: curl -X GET http://localhost:8181/api/catalog/v1/bronze/namespaces/raw -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace
    Returns: bool: True если существует, False если 404
    Example: exists = namespace_exists(session, "bronze", "raw")
    Реализация: HEAD в Polaris 1.4.0 возвращает 204 для существующего и подвисает на 5+ сек
    перед 404 для несуществующего. GET отвечает мгновенно в обоих случаях, поэтому используется он.
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/namespaces/{namespace}"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    if response.status_code == 404:
        return False
    response.raise_for_status()
    return True


def update_namespace_properties(
    session: PolarisSession,
    catalog_name: str,
    namespace: str,
    updates: dict | None = None,
    removals: list[str] | None = None,
) -> dict:
    """Обновить или удалить свойства namespace.
    Curl: curl -X POST http://localhost:8181/api/catalog/v1/bronze/namespaces/raw/properties -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"updates":{"key":"value"},"removals":[]}'
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace
        updates: словарь обновляемых свойств
        removals: список удаляемых ключей
    Returns: dict: updated, removed, missing
    Example: update_namespace_properties(session, "bronze", "raw", updates={"owner": "ivan"})
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/namespaces/{namespace}/properties"
    body: dict = {}
    if updates:
        body["updates"] = updates
    if removals:
        body["removals"] = removals
    response = requests.post(url, json=body, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def drop_namespace(session: PolarisSession, catalog_name: str, namespace: str) -> None:
    """⚠️ Удалить namespace. Namespace должен быть пустым (без таблиц и views).
    Curl: curl -X DELETE http://localhost:8181/api/catalog/v1/bronze/namespaces/raw -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace
    Returns: None
    Example: drop_namespace(session, "bronze", "raw")
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/namespaces/{namespace}"
    response = requests.delete(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()


# =============================================================================
# Tables
# =============================================================================


def list_tables(session: PolarisSession, catalog_name: str, namespace: str) -> list[str]:
    """Получить список таблиц в namespace.
    Curl: curl -X GET http://localhost:8181/api/catalog/v1/bronze/namespaces/raw/tables -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace
    Returns: list[str]: список имён таблиц
    Example: tables = list_tables(session, "bronze", "raw")
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/namespaces/{namespace}/tables"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    identifiers = response.json().get("identifiers", [])
    return [item["name"] for item in identifiers]


def create_table(
    session: PolarisSession,
    catalog_name: str,
    namespace: str,
    table_name: str,
    schema: dict,
    partition_spec: dict | None = None,
    write_order: dict | None = None,
    stage_create: bool = False,
    properties: dict | None = None,
) -> dict:
    """Создать новую Iceberg таблицу в namespace.
    Curl: curl -X POST http://localhost:8181/api/catalog/v1/bronze/namespaces/raw/tables -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"name":"orders","schema":{"type":"struct","fields":[...]}}'
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace
        table_name: имя таблицы
        schema: Iceberg schema (type, fields, schema-id и т.д.)
        partition_spec: спецификация партиционирования (опционально)
        write_order: порядок сортировки при записи (опционально)
        stage_create: True для staged create (двухфазное создание)
        properties: дополнительные свойства таблицы
    Returns: dict: LoadTableResult (metadata-location, metadata, config)
    Example: create_table(session, "bronze", "raw", "orders", schema={"type":"struct","fields":[]})
    Частично протестировано: базовый вызов (schema без partition_spec/write_order/stage_create) работает.
    Параметры partition_spec, write_order, stage_create=True не тестировались.
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/namespaces/{namespace}/tables"
    body: dict = {"name": table_name, "schema": schema, "stage-create": stage_create}
    if partition_spec:
        body["partition-spec"] = partition_spec
    if write_order:
        body["write-order"] = write_order
    if properties:
        body["properties"] = properties
    response = requests.post(url, json=body, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def get_table_metadata(
    session: PolarisSession, catalog_name: str, namespace: str, table_name: str
) -> dict:
    """Загрузить метаданные Iceberg таблицы (schema, snapshots, partition spec).
    Curl: curl -X GET http://localhost:8181/api/catalog/v1/bronze/namespaces/raw/tables/orders -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace
        table_name: имя таблицы
    Returns: dict: LoadTableResult (metadata-location, metadata, config)
    Example: meta = get_table_metadata(session, "bronze", "raw", "orders")
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/namespaces/{namespace}/tables/{table_name}"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def commit_table(
    session: PolarisSession,
    catalog_name: str,
    namespace: str,
    table_name: str,
    requirements: list[dict],
    updates: list[dict],
) -> dict:
    """Закоммитить изменения в таблицу через Iceberg REST (schema, snapshot, properties).
    Curl: curl -X POST http://localhost:8181/api/catalog/v1/bronze/namespaces/raw/tables/orders -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"identifier":{...},"requirements":[...],"updates":[...]}'
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace
        table_name: имя таблицы
        requirements: список предусловий (AssertCurrentSchemaId, AssertTableUUID и т.д.)
        updates: список изменений (AddSchemaUpdate, SetCurrentSchemaUpdate и т.д.)
    Returns: dict: CommitTableResponse (metadata-location, metadata)
    Example: commit_table(session, "bronze", "raw", "orders", requirements=[], updates=[{"action":"set-properties","updates":{}}])
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/namespaces/{namespace}/tables/{table_name}"
    body = {
        "identifier": {"namespace": [namespace], "name": table_name},
        "requirements": requirements,
        "updates": updates,
    }
    response = requests.post(url, json=body, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def table_exists(
    session: PolarisSession, catalog_name: str, namespace: str, table_name: str
) -> bool:
    """Проверить существование таблицы через GET с обработкой 404.
    Curl: curl -X GET http://localhost:8181/api/catalog/v1/bronze/namespaces/raw/tables/orders -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace
        table_name: имя таблицы
    Returns: bool: True если существует, False если 404
    Example: exists = table_exists(session, "bronze", "raw", "orders")
    Реализация: HEAD в Polaris 1.4.0 подвисает на 5+ сек перед 404. GET отвечает мгновенно.
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/namespaces/{namespace}/tables/{table_name}"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    if response.status_code == 404:
        return False
    response.raise_for_status()
    return True


def register_table(
    session: PolarisSession,
    catalog_name: str,
    namespace: str,
    table_name: str,
    metadata_location: str,
) -> dict:
    """Зарегистрировать существующую Iceberg таблицу по пути к metadata.json на S3.
    Curl: curl -X POST http://localhost:8181/api/catalog/v1/bronze/namespaces/raw/register -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"name":"orders","metadata-location":"s3://bucket/bronze/.../v1.metadata.json"}'
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace
        table_name: имя таблицы в каталоге
        metadata_location: путь к metadata.json на S3
    Returns: dict: LoadTableResult
    Example: register_table(session, "bronze", "raw", "orders", "s3://bucket/bronze/.../v1.metadata.json")
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/namespaces/{namespace}/register"
    body = {"name": table_name, "metadata-location": metadata_location}
    response = requests.post(url, json=body, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def rename_table(
    session: PolarisSession,
    catalog_name: str,
    namespace: str,
    old_name: str,
    new_name: str,
) -> None:
    """Переименовать таблицу в рамках одного namespace.
    Curl: curl -X POST http://localhost:8181/api/catalog/v1/bronze/tables/rename -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"source":{"namespace":["raw"],"name":"orders_old"},"destination":{"namespace":["raw"],"name":"orders"}}'
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace (одинаковый для source и destination)
        old_name: текущее имя таблицы
        new_name: новое имя таблицы
    Returns: None
    Example: rename_table(session, "bronze", "raw", "orders_old", "orders")
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/tables/rename"
    body = {
        "source": {"namespace": [namespace], "name": old_name},
        "destination": {"namespace": [namespace], "name": new_name},
    }
    response = requests.post(url, json=body, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()


def drop_table(
    session: PolarisSession,
    catalog_name: str,
    namespace: str,
    table_name: str,
    purge: bool = False,
) -> None:
    """⚠️ Удалить таблицу из каталога. При purge=True удаляются и файлы данных в S3.
    Curl: curl -X DELETE "http://localhost:8181/api/catalog/v1/bronze/namespaces/raw/tables/orders?purgeRequested=false" -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace
        table_name: имя таблицы
        purge: True — удалить файлы данных в S3, False — только запись в каталоге
    Returns: None
    Example: drop_table(session, "bronze", "raw", "orders", purge=False)
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/namespaces/{namespace}/tables/{table_name}"
    response = requests.delete(url, params={"purgeRequested": str(purge).lower()}, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()


# =============================================================================
# Views
# =============================================================================


def list_views(session: PolarisSession, catalog_name: str, namespace: str) -> list[str]:
    """Получить список views в namespace.
    Curl: curl -X GET http://localhost:8181/api/catalog/v1/bronze/namespaces/raw/views -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace
    Returns: list[str]: список имён views
    Example: views = list_views(session, "bronze", "raw")
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/namespaces/{namespace}/views"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    identifiers = response.json().get("identifiers", [])
    return [item["name"] for item in identifiers]


def create_view(
    session: PolarisSession,
    catalog_name: str,
    namespace: str,
    view_name: str,
    view_version: dict,
    schema: dict,
    properties: dict | None = None,
) -> dict:
    """Создать Iceberg view в namespace.
    Curl: curl -X POST http://localhost:8181/api/catalog/v1/bronze/namespaces/raw/views -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"name":"v_orders","view-version":{...},"schema":{...}}'
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace
        view_name: имя view
        view_version: ViewVersion (version-id, schema-id, representations, default-namespace и т.д.)
        schema: Iceberg schema (type, fields)
        properties: дополнительные свойства (опционально)
    Returns: dict: LoadViewResult (metadata-location, metadata)
    Example: create_view(session, "bronze", "raw", "v_orders", view_version={...}, schema={...})
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/namespaces/{namespace}/views"
    body: dict = {"name": view_name, "view-version": view_version, "schema": schema}
    if properties:
        body["properties"] = properties
    response = requests.post(url, json=body, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def get_view(
    session: PolarisSession, catalog_name: str, namespace: str, view_name: str
) -> dict:
    """Загрузить метаданные Iceberg view.
    Curl: curl -X GET http://localhost:8181/api/catalog/v1/bronze/namespaces/raw/views/v_orders -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace
        view_name: имя view
    Returns: dict: LoadViewResult (metadata-location, metadata)
    Example: view = get_view(session, "bronze", "raw", "v_orders")
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/namespaces/{namespace}/views/{view_name}"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def update_view(
    session: PolarisSession,
    catalog_name: str,
    namespace: str,
    view_name: str,
    requirements: list[dict],
    updates: list[dict],
) -> dict:
    """Закоммитить изменения в view через Iceberg REST.
    Curl: curl -X POST http://localhost:8181/api/catalog/v1/bronze/namespaces/raw/views/v_orders -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"identifier":{...},"requirements":[...],"updates":[...]}'
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace
        view_name: имя view
        requirements: список предусловий
        updates: список изменений (AddSchemaUpdate, AddViewVersionUpdate и т.д.)
    Returns: dict: CommitViewResponse (metadata-location, metadata)
    Example: update_view(session, "bronze", "raw", "v_orders", requirements=[], updates=[...])
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/namespaces/{namespace}/views/{view_name}"
    body = {
        "identifier": {"namespace": [namespace], "name": view_name},
        "requirements": requirements,
        "updates": updates,
    }
    response = requests.post(url, json=body, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def view_exists(
    session: PolarisSession, catalog_name: str, namespace: str, view_name: str
) -> bool:
    """Проверить существование view через GET с обработкой 404.
    Curl: curl -X GET http://localhost:8181/api/catalog/v1/bronze/namespaces/raw/views/v_orders -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace
        view_name: имя view
    Returns: bool: True если существует, False если 404
    Example: exists = view_exists(session, "bronze", "raw", "v_orders")
    Реализация: HEAD в Polaris 1.4.0 подвисает на 5+ сек перед 404. GET отвечает мгновенно.
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/namespaces/{namespace}/views/{view_name}"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    if response.status_code == 404:
        return False
    response.raise_for_status()
    return True


def rename_view(
    session: PolarisSession,
    catalog_name: str,
    namespace: str,
    old_name: str,
    new_name: str,
) -> None:
    """Переименовать view в рамках одного namespace.
    Curl: curl -X POST http://localhost:8181/api/catalog/v1/bronze/views/rename -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{"source":{"namespace":["raw"],"name":"v_old"},"destination":{"namespace":["raw"],"name":"v_new"}}'
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace
        old_name: текущее имя view
        new_name: новое имя view
    Returns: None
    Example: rename_view(session, "bronze", "raw", "v_old", "v_new")
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/views/rename"
    body = {
        "source": {"namespace": [namespace], "name": old_name},
        "destination": {"namespace": [namespace], "name": new_name},
    }
    response = requests.post(url, json=body, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()


def drop_view(
    session: PolarisSession, catalog_name: str, namespace: str, view_name: str
) -> None:
    """⚠️ Удалить view из каталога безвозвратно.
    Curl: curl -X DELETE http://localhost:8181/api/catalog/v1/bronze/namespaces/raw/views/v_orders -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        namespace: имя namespace
        view_name: имя view
    Returns: None
    Example: drop_view(session, "bronze", "raw", "v_orders")
    """
    url = f"{session.host}/api/catalog/v1/{catalog_name}/namespaces/{namespace}/views/{view_name}"
    response = requests.delete(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
