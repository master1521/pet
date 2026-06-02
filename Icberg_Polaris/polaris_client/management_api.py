"""
Management API — тонкий HTTP-клиент к Apache Polaris 1.4.0 Management API.

API: /api/management/v1/
Документация: https://raw.githubusercontent.com/apache/polaris/refs/tags/apache-polaris-1.4.0/spec/polaris-management-service.yml

Возможности:
  - Catalogs: list, get, create, update, delete
  - Principals: list, get, create, update, delete, rotate/reset credentials
  - Principal Roles: list, get, create, update, delete, assign/unassign
  - Catalog Roles: list, get, create, update, delete, assign/unassign
  - Grants: list, add, revoke

Использование:
    from polaris_client.session import PolarisSession
    from polaris_client.management_api import list_catalogs

    session = PolarisSession(host="http://localhost:8181", client_id="root", client_secret="secret")
    catalogs = list_catalogs(session)
"""

import requests

from polaris_client.session import PolarisSession


# ─── Группа 1 — Catalogs ─────────────────────────────────────────────────────


def list_catalogs(session: PolarisSession) -> list[dict]:
    """Получить список каталогов.
    Curl: curl -X GET http://localhost:8181/api/management/v1/catalogs -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
    Returns: list[dict]: список объектов каталогов
    Example: catalogs = list_catalogs(session)
    """
    url = f"{session.host}/api/management/v1/catalogs"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json().get("catalogs", [])


def get_catalog(session: PolarisSession, catalog_name: str) -> dict:
    """Получить каталог по имени.
    Curl: curl -X GET http://localhost:8181/api/management/v1/catalogs/{catalog_name} -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
    Returns: dict: объект каталога
    Example: catalog = get_catalog(session, "bronze")
    """
    url = f"{session.host}/api/management/v1/catalogs/{catalog_name}"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def create_catalog(
    session: PolarisSession,
    name: str,
    storage_location: str,
    s3_endpoint: str,
    s3_access_key: str,
    s3_secret_key: str,
    sts_unavailable: bool = True,
    path_style_access: bool = False,
) -> dict:
    """Создать новый каталог типа INTERNAL с S3-хранилищем.
    Curl: curl -X POST http://localhost:8181/api/management/v1/catalogs -H "Authorization: Bearer $TOKEN" -d '{...}'
    Args:
        session: активная сессия Polaris
        name: имя каталога
        storage_location: базовый S3-путь, например s3://bucket/prefix
        s3_endpoint: URL эндпоинта S3, например https://storage.yandexcloud.net
        s3_access_key: AWS access key ID
        s3_secret_key: AWS secret access key
        sts_unavailable: True если STS недоступен (Yandex Object Storage)
        path_style_access: использовать path-style URL для S3
    Returns: dict: созданный объект каталога
    Example: catalog = create_catalog(session, "bronze", "s3://bucket/bronze", "https://storage.yandexcloud.net", "key", "secret")
    """
    url = f"{session.host}/api/management/v1/catalogs"
    body = {
        "name": name,
        "type": "INTERNAL",
        "properties": {
            "default-base-location": storage_location,
            "s3.endpoint": s3_endpoint,
            "s3.path-style-access": str(path_style_access).lower(),
            "s3.access-key-id": s3_access_key,
            "s3.secret-access-key": s3_secret_key,
        },
        "storageConfigInfo": {
            "storageType": "S3",
            "allowedLocations": [storage_location],
            "endpoint": s3_endpoint,
            "pathStyleAccess": path_style_access,
            "stsUnavailable": sts_unavailable,
        },
    }
    response = requests.post(url, headers=session.headers, json=body, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def update_catalog(
    session: PolarisSession,
    catalog_name: str,
    properties: dict,
    storage_config: dict | None = None,
) -> dict:
    """Обновить свойства каталога. Автоматически получает currentEntityVersion через GET.
    Curl: curl -X PUT http://localhost:8181/api/management/v1/catalogs/{catalog_name} -H "Authorization: Bearer $TOKEN" -d '{"currentEntityVersion":1,"properties":{...}}'
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        properties: словарь новых свойств (мержится с текущими)
        storage_config: опциональный объект storageConfigInfo
    Returns: dict: обновлённый объект каталога
    Example: updated = update_catalog(session, "bronze", {"comment": "raw layer"})
    """
    current = get_catalog(session, catalog_name)
    entity_version = current.get("entityVersion", 1)
    merged_props = {**current.get("properties", {}), **properties}
    url = f"{session.host}/api/management/v1/catalogs/{catalog_name}"
    # Polaris UpdateCatalogRequest — плоская структура (currentEntityVersion + properties + storageConfigInfo)
    body: dict = {"currentEntityVersion": entity_version, "properties": merged_props}
    if storage_config is not None:
        body["storageConfigInfo"] = storage_config
    response = requests.put(url, headers=session.headers, json=body, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def delete_catalog(session: PolarisSession, catalog_name: str) -> None:
    """⚠️ Удалить каталог. Необратимая операция — метаданные каталога будут удалены.
    Curl: curl -X DELETE http://localhost:8181/api/management/v1/catalogs/{catalog_name} -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
    Returns: None
    Example: delete_catalog(session, "bronze")
    """
    url = f"{session.host}/api/management/v1/catalogs/{catalog_name}"
    response = requests.delete(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()


# ─── Группа 2 — Principals ───────────────────────────────────────────────────


def list_principals(session: PolarisSession) -> list[dict]:
    """Получить список principals.
    Curl: curl -X GET http://localhost:8181/api/management/v1/principals -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
    Returns: list[dict]: список объектов principals
    Example: principals = list_principals(session)
    """
    url = f"{session.host}/api/management/v1/principals"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json().get("principals", [])


def get_principal(session: PolarisSession, name: str) -> dict:
    """Получить principal по имени.
    Curl: curl -X GET http://localhost:8181/api/management/v1/principals/{name} -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        name: имя principal
    Returns: dict: объект principal
    Example: principal = get_principal(session, "ivan")
    """
    url = f"{session.host}/api/management/v1/principals/{name}"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def create_principal(
    session: PolarisSession,
    name: str,
    principal_type: str = "USER",
) -> dict:
    """Создать нового principal.
    Curl: curl -X POST http://localhost:8181/api/management/v1/principals -H "Authorization: Bearer $TOKEN" -d '{"name": "ivan", "type": "USER"}'
    Args:
        session: активная сессия Polaris
        name: имя principal
        principal_type: тип principal, по умолчанию "USER"
    Returns: dict: созданный объект principal с credentials
    Example: principal = create_principal(session, "ivan")
    """
    url = f"{session.host}/api/management/v1/principals"
    body = {"name": name, "type": principal_type}
    response = requests.post(url, headers=session.headers, json=body, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def update_principal(
    session: PolarisSession,
    name: str,
    properties: dict,
) -> dict:
    """Обновить свойства principal. Автоматически получает currentEntityVersion через GET.
    Curl: curl -X PUT http://localhost:8181/api/management/v1/principals/{name} -H "Authorization: Bearer $TOKEN" -d '{"currentEntityVersion":1,"principal":{"name":"ivan","type":"USER","properties":{...}}}'
    Args:
        session: активная сессия Polaris
        name: имя principal
        properties: словарь новых свойств
    Returns: dict: обновлённый объект principal
    Example: updated = update_principal(session, "ivan", {"email": "ivan@example.com"})
    """
    current = get_principal(session, name)
    url = f"{session.host}/api/management/v1/principals/{name}"
    body = {
        "currentEntityVersion": current.get("entityVersion", 1),
        "name": name,
        "type": current.get("type", "USER"),
        "properties": properties,
    }
    response = requests.put(url, headers=session.headers, json=body, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def delete_principal(session: PolarisSession, name: str) -> None:
    """⚠️ Удалить principal. Необратимая операция — пользователь потеряет доступ.
    Curl: curl -X DELETE http://localhost:8181/api/management/v1/principals/{name} -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        name: имя principal
    Returns: None
    Example: delete_principal(session, "ivan")
    """
    url = f"{session.host}/api/management/v1/principals/{name}"
    response = requests.delete(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()


def rotate_principal_credentials(session: PolarisSession, name: str) -> dict:
    """Ротировать credentials principal (старый секрет аннулируется).
    Curl: curl -X POST http://localhost:8181/api/management/v1/principals/{name}/rotate -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        name: имя principal
    Returns: dict: новые credentials principal
    Example: creds = rotate_principal_credentials(session, "ivan")
    Важно: Polaris запрещает root-сессии ротировать credentials чужого principal (403).
    Функция должна вызываться от имени самого principal с его собственными credentials.
    """
    url = f"{session.host}/api/management/v1/principals/{name}/rotate"
    response = requests.post(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def reset_principal_credentials(session: PolarisSession, name: str) -> dict:
    """Сбросить credentials principal до дефолтных.
    Curl: curl -X POST http://localhost:8181/api/management/v1/principals/{name}/reset -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        name: имя principal
    Returns: dict: новые credentials principal
    Example: creds = reset_principal_credentials(session, "ivan")
    """
    url = f"{session.host}/api/management/v1/principals/{name}/reset"
    response = requests.post(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


# ─── Группа 3 — Principal Roles (назначение на principals) ───────────────────


def list_principal_assigned_roles(
    session: PolarisSession,
    principal_name: str,
) -> list[dict]:
    """Получить список principal roles, назначенных на principal.
    Curl: curl -X GET http://localhost:8181/api/management/v1/principals/{principal_name}/principal-roles -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        principal_name: имя principal
    Returns: list[dict]: список назначенных principal roles
    Example: roles = list_principal_assigned_roles(session, "ivan")
    """
    url = f"{session.host}/api/management/v1/principals/{principal_name}/principal-roles"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json().get("roles", [])


def assign_role_to_principal(
    session: PolarisSession,
    principal_name: str,
    role_name: str,
) -> None:
    """Назначить principal role на principal.
    Curl: curl -X PUT http://localhost:8181/api/management/v1/principals/{principal_name}/principal-roles -H "Authorization: Bearer $TOKEN" -d '{"name": "data_engineer"}'
    Args:
        session: активная сессия Polaris
        principal_name: имя principal
        role_name: имя principal role для назначения
    Returns: None
    Example: assign_role_to_principal(session, "ivan", "data_engineer")
    """
    url = f"{session.host}/api/management/v1/principals/{principal_name}/principal-roles"
    body = {"name": role_name}
    response = requests.put(url, headers=session.headers, json=body, timeout=session.timeout)
    response.raise_for_status()


def unassign_role_from_principal(
    session: PolarisSession,
    principal_name: str,
    role_name: str,
) -> None:
    """Отвязать principal role от principal.
    Curl: curl -X DELETE http://localhost:8181/api/management/v1/principals/{principal_name}/principal-roles/{role_name} -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        principal_name: имя principal
        role_name: имя principal role для отвязки
    Returns: None
    Example: unassign_role_from_principal(session, "ivan", "data_engineer")
    """
    url = f"{session.host}/api/management/v1/principals/{principal_name}/principal-roles/{role_name}"
    response = requests.delete(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()


# ─── Группа 4 — Principal Roles (CRUD) ───────────────────────────────────────


def list_principal_roles(session: PolarisSession) -> list[dict]:
    """Получить список всех principal roles.
    Curl: curl -X GET http://localhost:8181/api/management/v1/principal-roles -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
    Returns: list[dict]: список объектов principal roles
    Example: roles = list_principal_roles(session)
    """
    url = f"{session.host}/api/management/v1/principal-roles"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json().get("roles", [])


def get_principal_role(session: PolarisSession, name: str) -> dict:
    """Получить principal role по имени.
    Curl: curl -X GET http://localhost:8181/api/management/v1/principal-roles/{name} -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        name: имя principal role
    Returns: dict: объект principal role
    Example: role = get_principal_role(session, "data_engineer")
    """
    url = f"{session.host}/api/management/v1/principal-roles/{name}"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def create_principal_role(session: PolarisSession, name: str) -> dict:
    """Создать новую principal role.
    Curl: curl -X POST http://localhost:8181/api/management/v1/principal-roles -H "Authorization: Bearer $TOKEN" -d '{"name": "data_engineer"}'
    Args:
        session: активная сессия Polaris
        name: имя новой principal role
    Returns: dict: созданный объект principal role
    Example: role = create_principal_role(session, "data_engineer")
    """
    url = f"{session.host}/api/management/v1/principal-roles"
    body = {"name": name}
    response = requests.post(url, headers=session.headers, json=body, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def update_principal_role(
    session: PolarisSession,
    name: str,
    properties: dict,
) -> dict:
    """Обновить свойства principal role. Автоматически получает currentEntityVersion через GET.
    Curl: curl -X PUT http://localhost:8181/api/management/v1/principal-roles/{name} -H "Authorization: Bearer $TOKEN" -d '{"currentEntityVersion":1,"principalRole":{"name":"data_engineer","properties":{...}}}'
    Args:
        session: активная сессия Polaris
        name: имя principal role
        properties: словарь новых свойств
    Returns: dict: обновлённый объект principal role
    Example: updated = update_principal_role(session, "data_engineer", {"description": "Engineers"})
    """
    current = get_principal_role(session, name)
    url = f"{session.host}/api/management/v1/principal-roles/{name}"
    body = {
        "currentEntityVersion": current.get("entityVersion", 1),
        "name": name,
        "properties": properties,
    }
    response = requests.put(url, headers=session.headers, json=body, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def delete_principal_role(session: PolarisSession, name: str) -> None:
    """⚠️ Удалить principal role. Необратимая операция — все связанные назначения будут разорваны.
    Curl: curl -X DELETE http://localhost:8181/api/management/v1/principal-roles/{name} -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        name: имя principal role
    Returns: None
    Example: delete_principal_role(session, "data_engineer")
    """
    url = f"{session.host}/api/management/v1/principal-roles/{name}"
    response = requests.delete(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()


def list_principals_for_role(
    session: PolarisSession,
    role_name: str,
) -> list[dict]:
    """Получить список principals, которым назначена principal role.
    Curl: curl -X GET http://localhost:8181/api/management/v1/principal-roles/{role_name}/principals -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        role_name: имя principal role
    Returns: list[dict]: список principals с данной ролью
    Example: principals = list_principals_for_role(session, "data_engineer")
    """
    url = f"{session.host}/api/management/v1/principal-roles/{role_name}/principals"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json().get("principals", [])


# ─── Группа 5 — Catalog Roles назначение на Principal Roles ──────────────────


def list_catalog_roles_for_principal_role(
    session: PolarisSession,
    principal_role_name: str,
    catalog_name: str,
) -> list[dict]:
    """Получить список catalog roles, назначенных на principal role для каталога.
    Curl: curl -X GET http://localhost:8181/api/management/v1/principal-roles/{principal_role_name}/catalog-roles/{catalog_name} -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        principal_role_name: имя principal role
        catalog_name: имя каталога
    Returns: list[dict]: список catalog roles
    Example: roles = list_catalog_roles_for_principal_role(session, "data_engineer", "bronze")
    """
    url = f"{session.host}/api/management/v1/principal-roles/{principal_role_name}/catalog-roles/{catalog_name}"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json().get("roles", [])


def assign_catalog_role_to_principal_role(
    session: PolarisSession,
    principal_role_name: str,
    catalog_name: str,
    catalog_role_name: str,
) -> None:
    """Назначить catalog role на principal role.
    Curl: curl -X PUT http://localhost:8181/api/management/v1/principal-roles/{principal_role_name}/catalog-roles/{catalog_name} -H "Authorization: Bearer $TOKEN" -d '{"catalogRole": {"name": "bronze_admin"}}'
    Args:
        session: активная сессия Polaris
        principal_role_name: имя principal role
        catalog_name: имя каталога
        catalog_role_name: имя catalog role для назначения
    Returns: None
    Example: assign_catalog_role_to_principal_role(session, "data_engineer", "bronze", "bronze_admin")
    """
    url = f"{session.host}/api/management/v1/principal-roles/{principal_role_name}/catalog-roles/{catalog_name}"
    body = {"catalogRole": {"name": catalog_role_name}}
    response = requests.put(url, headers=session.headers, json=body, timeout=session.timeout)
    response.raise_for_status()


def unassign_catalog_role_from_principal_role(
    session: PolarisSession,
    principal_role_name: str,
    catalog_name: str,
    catalog_role_name: str,
) -> None:
    """Отвязать catalog role от principal role.
    Curl: curl -X DELETE http://localhost:8181/api/management/v1/principal-roles/{principal_role_name}/catalog-roles/{catalog_name}/{catalog_role_name} -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        principal_role_name: имя principal role
        catalog_name: имя каталога
        catalog_role_name: имя catalog role для отвязки
    Returns: None
    Example: unassign_catalog_role_from_principal_role(session, "data_engineer", "bronze", "bronze_admin")
    """
    url = f"{session.host}/api/management/v1/principal-roles/{principal_role_name}/catalog-roles/{catalog_name}/{catalog_role_name}"
    response = requests.delete(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()


# ─── Группа 6 — Catalog Roles (CRUD) ─────────────────────────────────────────


def list_catalog_roles(session: PolarisSession, catalog_name: str) -> list[dict]:
    """Получить список catalog roles каталога.
    Curl: curl -X GET http://localhost:8181/api/management/v1/catalogs/{catalog_name}/catalog-roles -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
    Returns: list[dict]: список объектов catalog roles
    Example: roles = list_catalog_roles(session, "bronze")
    """
    url = f"{session.host}/api/management/v1/catalogs/{catalog_name}/catalog-roles"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json().get("roles", [])


def get_catalog_role(
    session: PolarisSession,
    catalog_name: str,
    role_name: str,
) -> dict:
    """Получить catalog role по имени.
    Curl: curl -X GET http://localhost:8181/api/management/v1/catalogs/{catalog_name}/catalog-roles/{role_name} -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        role_name: имя catalog role
    Returns: dict: объект catalog role
    Example: role = get_catalog_role(session, "bronze", "bronze_admin")
    """
    url = f"{session.host}/api/management/v1/catalogs/{catalog_name}/catalog-roles/{role_name}"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def create_catalog_role(
    session: PolarisSession,
    catalog_name: str,
    role_name: str,
) -> dict:
    """Создать новую catalog role в каталоге.
    Curl: curl -X POST http://localhost:8181/api/management/v1/catalogs/{catalog_name}/catalog-roles -H "Authorization: Bearer $TOKEN" -d '{"name": "bronze_admin"}'
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        role_name: имя новой catalog role
    Returns: dict: созданный объект catalog role
    Example: role = create_catalog_role(session, "bronze", "bronze_admin")
    """
    url = f"{session.host}/api/management/v1/catalogs/{catalog_name}/catalog-roles"
    body = {"name": role_name}
    response = requests.post(url, headers=session.headers, json=body, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def update_catalog_role(
    session: PolarisSession,
    catalog_name: str,
    role_name: str,
    properties: dict,
) -> dict:
    """Обновить свойства catalog role. Автоматически получает currentEntityVersion через GET.
    Curl: curl -X PUT http://localhost:8181/api/management/v1/catalogs/{catalog_name}/catalog-roles/{role_name} -H "Authorization: Bearer $TOKEN" -d '{"currentEntityVersion":1,"catalogRole":{"name":"bronze_admin","properties":{...}}}'
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        role_name: имя catalog role
        properties: словарь новых свойств
    Returns: dict: обновлённый объект catalog role
    Example: updated = update_catalog_role(session, "bronze", "bronze_admin", {"description": "Admin"})
    """
    current = get_catalog_role(session, catalog_name, role_name)
    url = f"{session.host}/api/management/v1/catalogs/{catalog_name}/catalog-roles/{role_name}"
    body = {
        "currentEntityVersion": current.get("entityVersion", 1),
        "name": role_name,
        "properties": properties,
    }
    response = requests.put(url, headers=session.headers, json=body, timeout=session.timeout)
    response.raise_for_status()
    return response.json()


def delete_catalog_role(
    session: PolarisSession,
    catalog_name: str,
    role_name: str,
) -> None:
    """⚠️ Удалить catalog role. Необратимая операция — все grants и назначения роли будут удалены.
    Curl: curl -X DELETE http://localhost:8181/api/management/v1/catalogs/{catalog_name}/catalog-roles/{role_name} -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        role_name: имя catalog role
    Returns: None
    Example: delete_catalog_role(session, "bronze", "bronze_admin")
    """
    url = f"{session.host}/api/management/v1/catalogs/{catalog_name}/catalog-roles/{role_name}"
    response = requests.delete(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()


# ─── Группа 7 — Grants ───────────────────────────────────────────────────────


def list_grants(
    session: PolarisSession,
    catalog_name: str,
    role_name: str,
) -> list[dict]:
    """Получить список grants для catalog role.
    Curl: curl -X GET http://localhost:8181/api/management/v1/catalogs/{catalog_name}/catalog-roles/{role_name}/grants -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        role_name: имя catalog role
    Returns: list[dict]: список объектов grants
    Example: grants = list_grants(session, "bronze", "bronze_admin")
    """
    url = f"{session.host}/api/management/v1/catalogs/{catalog_name}/catalog-roles/{role_name}/grants"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json().get("grants", [])


def add_grant(
    session: PolarisSession,
    catalog_name: str,
    role_name: str,
    privilege: str,
    grant_type: str = "catalog",
    namespace: list[str] | None = None,
    name: str | None = None,
) -> None:
    """Добавить grant (привилегию) на catalog role.
    Уровни grant_type:
      "catalog"   — привилегия на весь каталог (CATALOG_MANAGE_CONTENT и т.д.)
      "namespace" — на namespace, требует namespace=["orders"]
      "table"     — на таблицу, требует namespace=["orders"], name="raw_orders"
      "view"      — на view, требует namespace=["orders"], name="v_orders"
    Curl: curl -X PUT http://localhost:8181/api/management/v1/catalogs/{catalog_name}/catalog-roles/{role_name}/grants -H "Authorization: Bearer $TOKEN" -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}'
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        role_name: имя catalog role
        privilege: привилегия, например CATALOG_MANAGE_CONTENT, TABLE_READ_DATA
        grant_type: уровень гранта — "catalog" | "namespace" | "table" | "view"
        namespace: список частей namespace, например ["orders"] (для table/view/namespace)
        name: имя таблицы или view (только для grant_type="table" или "view")
    Returns: None
    Example: add_grant(session, "bronze", "bronze_admin", "CATALOG_MANAGE_CONTENT")
    Example: add_grant(session, "bronze", "bronze_reader", "TABLE_READ_DATA", grant_type="table", namespace=["orders"], name="raw_orders")
    """
    url = f"{session.host}/api/management/v1/catalogs/{catalog_name}/catalog-roles/{role_name}/grants"
    grant: dict = {"type": grant_type, "privilege": privilege}
    if namespace is not None:
        grant["namespace"] = namespace
    if name is not None:
        if grant_type == "table":
            grant["tableName"] = name
        elif grant_type == "view":
            grant["viewName"] = name
        else:
            grant["name"] = name
    response = requests.put(url, headers=session.headers, json={"grant": grant}, timeout=session.timeout)
    response.raise_for_status()


def revoke_grant(
    session: PolarisSession,
    catalog_name: str,
    role_name: str,
    privilege: str,
    grant_type: str = "catalog",
    namespace: list[str] | None = None,
    name: str | None = None,
) -> None:
    """Отозвать grant (привилегию) у catalog role.
    Polaris использует POST для revoke (не DELETE) — особенность Management API v1.
    Уровни grant_type: "catalog" | "namespace" | "table" | "view" — аналогично add_grant.
    Curl: curl -X POST http://localhost:8181/api/management/v1/catalogs/{catalog_name}/catalog-roles/{role_name}/grants -H "Authorization: Bearer $TOKEN" -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}'
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        role_name: имя catalog role
        privilege: привилегия для отзыва, например CATALOG_MANAGE_CONTENT
        grant_type: уровень гранта — "catalog" | "namespace" | "table" | "view"
        namespace: список частей namespace, например ["orders"] (для table/view/namespace)
        name: имя таблицы или view (только для grant_type="table" или "view")
    Returns: None
    Example: revoke_grant(session, "bronze", "bronze_admin", "CATALOG_MANAGE_CONTENT")
    Example: revoke_grant(session, "bronze", "bronze_reader", "TABLE_READ_DATA", grant_type="table", namespace=["orders"], name="raw_orders")
    """
    url = f"{session.host}/api/management/v1/catalogs/{catalog_name}/catalog-roles/{role_name}/grants"
    grant: dict = {"type": grant_type, "privilege": privilege}
    if namespace is not None:
        grant["namespace"] = namespace
    if name is not None:
        if grant_type == "table":
            grant["tableName"] = name
        elif grant_type == "view":
            grant["viewName"] = name
        else:
            grant["name"] = name
    response = requests.post(url, headers=session.headers, json={"grant": grant}, timeout=session.timeout)
    response.raise_for_status()


def list_principal_roles_for_catalog_role(
    session: PolarisSession,
    catalog_name: str,
    role_name: str,
) -> list[dict]:
    """Получить список principal roles, которым назначена catalog role.
    Curl: curl -X GET http://localhost:8181/api/management/v1/catalogs/{catalog_name}/catalog-roles/{role_name}/principal-roles -H "Authorization: Bearer $TOKEN"
    Args:
        session: активная сессия Polaris
        catalog_name: имя каталога
        role_name: имя catalog role
    Returns: list[dict]: список principal roles
    Example: principal_roles = list_principal_roles_for_catalog_role(session, "bronze", "bronze_admin")
    """
    url = f"{session.host}/api/management/v1/catalogs/{catalog_name}/catalog-roles/{role_name}/principal-roles"
    response = requests.get(url, headers=session.headers, timeout=session.timeout)
    response.raise_for_status()
    return response.json().get("principalRoles", [])
