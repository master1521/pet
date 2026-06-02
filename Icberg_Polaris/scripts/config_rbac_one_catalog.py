"""
╔══════════════════════════════════════════════════════════════╗
║           DATA LAKEHOUSE — ПОЛНАЯ СТРУКТУРА СТЕНДА           ║
╚══════════════════════════════════════════════════════════════╝

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ХРАНИЛИЩЕ — Yandex Object Storage
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
s3://my-bucket/
└── lakehouse/
  ├── bronze/
  ├── silver/
  └── gold/

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
КАТАЛОГ — Apache Polaris
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
catalog: lakehouse
│   storage_location: s3://my-bucket/lakehouse/
│
├── namespace: bronze
│   ├── raw_categories      (5 строк — справочник категорий)
│   ├── raw_products        (100 строк — 5% null, 10% inactive)
│   ├── raw_customers       (1 030 строк — с дублями)
│   ├── raw_orders          (20 000 строк — 2% невалидных статусов)
│   └── raw_order_items     (60 041 строка)
│
├── namespace: silver
│   ├── customers           (1 000 строк — дедупликация)
│   ├── products            (81 строка — только активные)
│   ├── orders              (19 608 строк — только валидные)
│   └── order_items         (60 041 строка — с line_total)
│
└── namespace: gold
  ├── mart_sales_by_category  (65 строк — выручка по категориям)
  └── mart_top_customers      (1 000 строк — RFM High/Mid/Low)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PRINCIPALS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
├── ivan        (USER)    → data_engineer
├── maria       (USER)    → data_analyst
├── alex        (USER)    → data_scientist
├── etl_service (USER)    → etl_service
└── bi_user     (USER)    → viewer_gold

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PRINCIPAL ROLES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
├── data_engineer  — полный контроль каталога, весь pipeline
├── data_analyst   — читает silver, читает и пишет gold
├── viewer_gold    — только чтение gold, для BI и бизнес-пользователей
├── data_scientist — читает все слои, ничего не пишет
└── etl_service    — пишет в bronze, читает bronze, пишет в silver

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CATALOG ROLES И ПРИВИЛЕГИИ (catalog: lakehouse)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
engineer_catalog_role
│   Может всё: создавать/удалять таблицы и неймспейсы, читать
│   и писать данные, управлять доступами других ролей.
│   Полный контроль каталога.
├── [catalog]    CATALOG_MANAGE_CONTENT
└── [catalog]    CATALOG_MANAGE_ACCESS

analyst_catalog_role
│   Читает чистые таблицы из silver для построения отчётов.
│   В gold — полные права: создаёт собственные витрины,
│   обновляет данные. Bronze закрыт — грязный слой до
│   трансформации.
├── [catalog]    NAMESPACE_LIST
├── [ns: silver] TABLE_LIST, TABLE_READ_DATA, TABLE_FULL_METADATA
└── [ns: gold]   TABLE_CREATE, TABLE_LIST, TABLE_READ_DATA,
               TABLE_WRITE_DATA, TABLE_FULL_METADATA

viewer_catalog_role
│   Только просмотр готовых витрин gold. Не может ничего
│   изменять или создавать. Подходит для BI-инструментов
│   и бизнес-пользователей.
├── [catalog]    NAMESPACE_LIST
└── [ns: gold]   TABLE_LIST, TABLE_READ_DATA, TABLE_FULL_METADATA

data_scientist_catalog_role
│   Читает данные из всех трёх слоёв. Ничего не пишет
│   и не создаёт. Нужен для разведочного анализа сырых
│   данных и подготовки датасетов для ML.
├── [catalog]    NAMESPACE_LIST
├── [ns: bronze] TABLE_LIST, TABLE_READ_DATA, TABLE_FULL_METADATA
├── [ns: silver] TABLE_LIST, TABLE_READ_DATA, TABLE_FULL_METADATA
└── [ns: gold]   TABLE_LIST, TABLE_READ_DATA, TABLE_FULL_METADATA

etl_service_catalog_role
│   Сервисный аккаунт для автоматизированных ETL-джобов
│   (Airflow, Spark). Пишет сырые данные в bronze, читает
│   bronze для трансформации, пишет результат в silver.
│   Gold не доступен — за витрины отвечает аналитик.
├── [catalog]    NAMESPACE_LIST
├── [ns: bronze] TABLE_CREATE, TABLE_LIST, TABLE_READ_DATA,
│               TABLE_WRITE_DATA, TABLE_FULL_METADATA
└── [ns: silver] TABLE_CREATE, TABLE_LIST, TABLE_READ_DATA,
               TABLE_WRITE_DATA, TABLE_FULL_METADATA

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PRINCIPAL ROLE > CATALOG ROLE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
├── data_engineer  → engineer_catalog_role
├── data_analyst   → analyst_catalog_role
├── viewer_gold    → viewer_catalog_role
├── data_scientist → data_scientist_catalog_role
└── etl_service    → etl_service_catalog_role

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
МАТРИЦА ДОСТУПОВ
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
namespace | data_engineer | data_analyst   | viewer_gold    | data_scientist | etl_service
----------|---------------|----------------|----------------|----------------|------------------
bronze    | полный доступ | нет доступа    | нет доступа    | только чтение  | чтение + запись
silver    | полный доступ | только чтение  | нет доступа    | только чтение  | чтение + запись
gold      | полный доступ | чтение + запись| только чтение  | только чтение  | нет доступа

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ИТОГО
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- 1 каталог:        lakehouse
- 3 namespace:      bronze, silver, gold
- 11 таблиц:        5 bronze + 4 silver + 2 gold
- 5 principals:     ivan, maria, alex, etl_service, bi_user
- 5 principal roles: data_engineer, data_analyst, viewer_gold, data_scientist, etl_service
- 5 catalog roles:  engineer_, analyst_, viewer_, data_scientist_, etl_service_catalog_role


ЗАПУСК
uv run python scripts/config_rbac_one_catalog.py

"""


import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from polaris_client import (
    PolarisSession,
    create_catalog,
    create_namespace,
    create_principal_role,
    create_principal,
    assign_role_to_principal,
    create_catalog_role,
    add_grant,
    assign_catalog_role_to_principal_role,
    list_catalogs,
    list_namespaces,
    list_principals,
    list_principal_roles,
    list_catalog_roles,
    list_grants,
)
from pprint import pp
from dotenv import dotenv_values

CATALOG_NAME: str = "lakehouse"

NAMESPACES: list[str] = ["bronze", "silver", "gold"]

PRINCIPALS: dict[str, str] = {
    "ivan":        "USER",
    "maria":       "USER",
    "alex":        "USER",
    "etl_service": "USER",
    "bi_user":     "USER",
}

PRINCIPAL_ROLES: list[str] = [
    "data_engineer",
    "data_analyst",
    "viewer_gold",
    "data_scientist",
    "etl_service",
]

PRINCIPAL_PRINCIPAL_ROLE: dict[str, str] = {
    "ivan":        "data_engineer",
    "maria":       "data_analyst",
    "alex":        "data_scientist",
    "etl_service": "etl_service",
    "bi_user":     "viewer_gold",
}

# catalog_role > список грантов: privilege + grant_type + namespace (list[str] | None)
CATALOG_ROLE_GRANTS: dict[str, list[dict]] = {
    "engineer_catalog_role": [
        {"privilege": "CATALOG_MANAGE_CONTENT", "grant_type": "catalog",    "namespace": None},
        {"privilege": "CATALOG_MANAGE_ACCESS",  "grant_type": "catalog",    "namespace": None},
    ],
    "analyst_catalog_role": [
        {"privilege": "NAMESPACE_LIST",  "grant_type": "catalog",    "namespace": None},
        {"privilege": "TABLE_LIST",      "grant_type": "namespace",  "namespace": ["silver"]},
        {"privilege": "TABLE_READ_DATA", "grant_type": "namespace",  "namespace": ["silver"]},
        {"privilege": "TABLE_CREATE",    "grant_type": "namespace",  "namespace": ["gold"]},
        {"privilege": "TABLE_LIST",      "grant_type": "namespace",  "namespace": ["gold"]},
        {"privilege": "TABLE_READ_DATA", "grant_type": "namespace",  "namespace": ["gold"]},
        {"privilege": "TABLE_WRITE_DATA","grant_type": "namespace",  "namespace": ["gold"]},
    ],
    "viewer_catalog_role": [
        {"privilege": "NAMESPACE_LIST",  "grant_type": "catalog",    "namespace": None},
        {"privilege": "TABLE_LIST",      "grant_type": "namespace",  "namespace": ["gold"]},
        {"privilege": "TABLE_READ_DATA", "grant_type": "namespace",  "namespace": ["gold"]},
    ],
    "data_scientist_catalog_role": [
        {"privilege": "NAMESPACE_LIST",  "grant_type": "catalog",    "namespace": None},
        {"privilege": "TABLE_LIST",      "grant_type": "namespace",  "namespace": ["bronze"]},
        {"privilege": "TABLE_READ_DATA", "grant_type": "namespace",  "namespace": ["bronze"]},
        {"privilege": "TABLE_LIST",      "grant_type": "namespace",  "namespace": ["silver"]},
        {"privilege": "TABLE_READ_DATA", "grant_type": "namespace",  "namespace": ["silver"]},
        {"privilege": "TABLE_LIST",      "grant_type": "namespace",  "namespace": ["gold"]},
        {"privilege": "TABLE_READ_DATA", "grant_type": "namespace",  "namespace": ["gold"]},
    ],
    "etl_service_catalog_role": [
        {"privilege": "NAMESPACE_LIST",          "grant_type": "catalog",    "namespace": None},
        {"privilege": "TABLE_CREATE",            "grant_type": "namespace",  "namespace": ["bronze"]},
        {"privilege": "TABLE_LIST",              "grant_type": "namespace",  "namespace": ["bronze"]},
        {"privilege": "TABLE_READ_DATA",         "grant_type": "namespace",  "namespace": ["bronze"]},
        {"privilege": "TABLE_WRITE_DATA",        "grant_type": "namespace",  "namespace": ["bronze"]},
        {"privilege": "TABLE_FULL_METADATA",     "grant_type": "namespace",  "namespace": ["bronze"]},
        {"privilege": "TABLE_CREATE",            "grant_type": "namespace",  "namespace": ["silver"]},
        {"privilege": "TABLE_LIST",              "grant_type": "namespace",  "namespace": ["silver"]},
        {"privilege": "TABLE_READ_DATA",         "grant_type": "namespace",  "namespace": ["silver"]},
        {"privilege": "TABLE_WRITE_DATA",        "grant_type": "namespace",  "namespace": ["silver"]},
        {"privilege": "TABLE_FULL_METADATA",     "grant_type": "namespace",  "namespace": ["silver"]},
    ],
}

# principal_role > catalog_role
PRINCIPAL_ROLE_CATALOG_ROLE: dict[str, str] = {
    "data_engineer":  "engineer_catalog_role",
    "data_analyst":   "analyst_catalog_role",
    "viewer_gold":    "viewer_catalog_role",
    "data_scientist": "data_scientist_catalog_role",
    "etl_service":    "etl_service_catalog_role",
}

# =============================================================================
# Функции настройки
# =============================================================================

def create_lakehouse_catalog(
    session: PolarisSession,
    storage_location: str,
    s3_endpoint: str,
    s3_access_key: str,
    s3_secret_key: str,
) -> None:
    """Создать единственный каталог lakehouse.
    Args:
        session: активная сессия Polaris
        storage_location: базовый S3-путь (до lakehouse/)
        s3_endpoint: URL эндпоинта S3
        s3_access_key: AWS access key ID
        s3_secret_key: AWS secret access key
    Returns: None
    Example: create_lakehouse_catalog(session, "s3://bucket", "https://storage.yandexcloud.net", "key_id", "secret")
    """
    base = storage_location.rstrip("/")
    pp(create_catalog(
        session,
        name=CATALOG_NAME,
        storage_location=f"{base}/{CATALOG_NAME}/",
        s3_endpoint=s3_endpoint,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
    ))


def create_principals(session: PolarisSession) -> None:
    """Создать Principals из PRINCIPALS.
    Args:
        session: активная сессия Polaris
    Returns: None
    Example: create_principals(session)
    """
    for principal_name, principal_type in PRINCIPALS.items():
        pp(create_principal(session, principal_name, principal_type))


def create_principal_roles(session: PolarisSession) -> None:
    """Создать Principal Roles из PRINCIPAL_ROLES.
    Args:
        session: активная сессия Polaris
    Returns: None
    Example: create_principal_roles(session)
    """
    for role_name in PRINCIPAL_ROLES:
        pp(create_principal_role(session, role_name))


def assign_principal_roles_to_principals(session: PolarisSession) -> None:
    """Назначить Principal Roles на Principals из PRINCIPAL_PRINCIPAL_ROLE.
    Args:
        session: активная сессия Polaris
    Returns: None
    Example: assign_principal_roles_to_principals(session)
    """
    for principal_name, principal_role_name in PRINCIPAL_PRINCIPAL_ROLE.items():
        pp(assign_role_to_principal(session, principal_name, principal_role_name))


def create_namespaces(session: PolarisSession) -> None:
    """Создать namespaces bronze, silver, gold в каталоге lakehouse.
    Polaris 1.4.0 требует существования namespace до назначения namespace-уровневых грантов.
    Args:
        session: активная сессия Polaris
    Returns: None
    Example: create_namespaces(session)
    """
    for ns in NAMESPACES:
        pp(create_namespace(session, CATALOG_NAME, ns))


def create_catalog_roles(session: PolarisSession) -> None:
    """Создать Catalog Roles из CATALOG_ROLE_GRANTS.
    Args:
        session: активная сессия Polaris
    Returns: None
    Example: create_catalog_roles(session)
    """
    for role_name in CATALOG_ROLE_GRANTS:
        pp(create_catalog_role(session, CATALOG_NAME, role_name))


def grant_privileges_to_catalog_roles(session: PolarisSession) -> None:
    """Выдать Privileges на Catalog Roles из CATALOG_ROLE_GRANTS.
    Поддерживает catalog-уровень и namespace-уровень через grant_type + namespace.
    Args:
        session: активная сессия Polaris
    Returns: None
    Example: grant_privileges_to_catalog_roles(session)
    """
    for role_name, grants in CATALOG_ROLE_GRANTS.items():
        for grant in grants:
            add_grant(
                session,
                CATALOG_NAME,
                role_name,
                grant["privilege"],
                grant_type=grant["grant_type"],
                namespace=grant["namespace"],
                name=grant.get("name"),
            )


def assign_catalog_roles_to_principal_roles(session: PolarisSession) -> None:
    """Назначить Catalog Roles на Principal Roles из PRINCIPAL_ROLE_CATALOG_ROLE.
    Args:
        session: активная сессия Polaris
    Returns: None
    Example: assign_catalog_roles_to_principal_roles(session)
    """
    for principal_role_name, catalog_role_name in PRINCIPAL_ROLE_CATALOG_ROLE.items():
        pp(assign_catalog_role_to_principal_role(
            session,
            principal_role_name,
            CATALOG_NAME,
            catalog_role_name,
        ))


def verify(session: PolarisSession) -> None:
    """Проверить результат настройки и вывести отчёт.
    Args:
        session: активная сессия Polaris
    Returns: None
    Example: verify(session)
    """
    print("\n" + "=" * 60)
    print("ПРОВЕРКА: Каталог")
    print("=" * 60)
    existing_catalogs = [c["name"] for c in list_catalogs(session)]
    status = "OK" if CATALOG_NAME in existing_catalogs else "ОТСУТСТВУЕТ"
    print(f"  [{status}] {CATALOG_NAME}")

    print("\n" + "=" * 60)
    print("ПРОВЕРКА: Namespaces")
    print("=" * 60)
    existing_ns = list_namespaces(session, CATALOG_NAME)
    for ns in NAMESPACES:
        status = "OK" if ns in existing_ns else "ОТСУТСТВУЕТ"
        print(f"  [{status}] {CATALOG_NAME}.{ns}")

    print("\n" + "=" * 60)
    print("ПРОВЕРКА: Principal Roles")
    print("=" * 60)
    existing_pr = [r["name"] for r in list_principal_roles(session)]
    for name in PRINCIPAL_ROLES:
        status = "OK" if name in existing_pr else "ОТСУТСТВУЕТ"
        print(f"  [{status}] {name}")

    print("\n" + "=" * 60)
    print("ПРОВЕРКА: Principals")
    print("=" * 60)
    existing_p = [p["name"] for p in list_principals(session)]
    for name in PRINCIPALS:
        status = "OK" if name in existing_p else "ОТСУТСТВУЕТ"
        print(f"  [{status}] {name}")

    print("\n" + "=" * 60)
    print("ПРОВЕРКА: Catalog Roles и Grants")
    print("=" * 60)
    existing_roles = [r["name"] for r in list_catalog_roles(session, CATALOG_NAME)]
    for role_name, expected_grants in CATALOG_ROLE_GRANTS.items():
        role_status = "OK" if role_name in existing_roles else "ОТСУТСТВУЕТ"
        actual_grants = list_grants(session, CATALOG_NAME, role_name)
        actual_keys = {
            (g.get("privilege"), g.get("type"), tuple(g["namespace"]) if g.get("namespace") else ())
            for g in actual_grants
        }
        expected_keys = {
            (g["privilege"], g["grant_type"], tuple(g["namespace"]) if g["namespace"] else ())
            for g in expected_grants
        }
        missing = expected_keys - actual_keys
        grants_status = "OK" if not missing else f"НЕТ: {missing}"
        print(f"  [{role_status}] {role_name}  grants=[{grants_status}]")


if __name__ == "__main__":
    env_path = Path(__file__).parent.parent.parent / ".env"
    cfg = dotenv_values(env_path)
    session = PolarisSession(
        host="http://localhost:8181",
        client_id=cfg["CLIENT_ID"],
        client_secret=cfg["CLIENT_SECRET"],
    )

    # 0) Создать Catalog
    create_lakehouse_catalog(
        session,
        cfg["STORAGE_LOCATION"],
        cfg["AWS_ENDPOINT_URL"],
        cfg["AWS_ACCESS_KEY_ID"],
        cfg["AWS_SECRET_ACCESS_KEY"],
    )

    # 1) Создать Principals
    create_principals(session)

    # 2) Создать Principal Roles
    create_principal_roles(session)

    # 3) Назначить Principal Roles > Principals
    assign_principal_roles_to_principals(session)

    # 4) Создать Catalog Roles
    create_catalog_roles(session)

    # 4.1) Создать Namespaces — нужны до namespace-уровневых грантов
    create_namespaces(session)

    # 5) Выдать Privileges > Catalog Roles
    grant_privileges_to_catalog_roles(session)

    # 6) Назначить Catalog Roles > Principal Roles
    assign_catalog_roles_to_principal_roles(session)

    # Проверка
    verify(session)
