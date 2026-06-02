"""
Настройка principal report_user с table-level доступом к gold.mart_top_customers.

Запускать ПОСЛЕ ноутбука lakehouse_bronze_silver_gold.ipynb —
Polaris 1.4.0 требует существования таблицы до выдачи table-level гранта.

Что создаёт:
- Principal: report_user (USER)
- Principal Role: report_viewer
- Catalog Role: report_catalog_role
- Привилегии:
    [catalog]  NAMESPACE_LIST
    [table]    TABLE_READ_DATA → lakehouse.gold.mart_top_customers
- Связи: report_user → report_viewer → report_catalog_role

Запуск: uv run python scripts/config_report_user.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from polaris_client import (
    PolarisSession,
    create_principal,
    create_principal_role,
    assign_role_to_principal,
    create_catalog_role,
    add_grant,
    assign_catalog_role_to_principal_role,
    list_principals,
    list_principal_roles,
    list_catalog_roles,
    list_grants,
)
from pprint import pp
from dotenv import dotenv_values

CATALOG_NAME: str = "lakehouse"
PRINCIPAL_NAME: str = "report_user"
PRINCIPAL_ROLE_NAME: str = "report_viewer"
CATALOG_ROLE_NAME: str = "report_catalog_role"


def setup_report_user(session: PolarisSession) -> None:
    """Создать report_user и настроить полную цепочку RBAC.
    Args:
        session: активная сессия Polaris
    Returns: None
    Example: setup_report_user(session)
    """
    # Создать Principal
    pp(create_principal(session, PRINCIPAL_NAME))

    # Создать Principal Role
    pp(create_principal_role(session, PRINCIPAL_ROLE_NAME))

    # Назначить Principal Role на Principal
    assign_role_to_principal(session, PRINCIPAL_NAME, PRINCIPAL_ROLE_NAME)

    # Создать Catalog Role
    pp(create_catalog_role(session, CATALOG_NAME, CATALOG_ROLE_NAME))

    # Выдать привилегии на Catalog Role
    add_grant(session, CATALOG_NAME, CATALOG_ROLE_NAME, "NAMESPACE_LIST", grant_type="catalog")
    add_grant(session, CATALOG_NAME, CATALOG_ROLE_NAME, "TABLE_READ_DATA", grant_type="table", namespace=["gold"], name="mart_top_customers")

    # Назначить Catalog Role на Principal Role
    assign_catalog_role_to_principal_role(session, PRINCIPAL_ROLE_NAME, CATALOG_NAME, CATALOG_ROLE_NAME)


def verify(session: PolarisSession) -> None:
    """Проверить результат настройки report_user.
    Args:
        session: активная сессия Polaris
    Returns: None
    Example: verify(session)
    """
    print("\n" + "=" * 60)
    print("ПРОВЕРКА: report_user")
    print("=" * 60)

    existing_principals = [p["name"] for p in list_principals(session)]
    status = "OK" if PRINCIPAL_NAME in existing_principals else "ОТСУТСТВУЕТ"
    print(f"  [{status}] principal: {PRINCIPAL_NAME}")

    existing_pr = [r["name"] for r in list_principal_roles(session)]
    status = "OK" if PRINCIPAL_ROLE_NAME in existing_pr else "ОТСУТСТВУЕТ"
    print(f"  [{status}] principal role: {PRINCIPAL_ROLE_NAME}")

    existing_cr = [r["name"] for r in list_catalog_roles(session, CATALOG_NAME)]
    status = "OK" if CATALOG_ROLE_NAME in existing_cr else "ОТСУТСТВУЕТ"
    print(f"  [{status}] catalog role: {CATALOG_ROLE_NAME}")

    grants = list_grants(session, CATALOG_NAME, CATALOG_ROLE_NAME)
    actual = {
        (g.get("privilege"), g.get("type"), tuple(g["namespace"]) if g.get("namespace") else ())
        for g in grants
    }
    for expected in [
        ("NAMESPACE_LIST", "catalog", ()),
        ("TABLE_READ_DATA", "table", ("gold",)),
    ]:
        status = "OK" if expected in actual else "ОТСУТСТВУЕТ"
        print(f"  [{status}] grant: {expected[0]} [{expected[1]}]")


if __name__ == "__main__":
    env_path = Path(__file__).parent.parent.parent / ".env"
    cfg = dotenv_values(env_path)
    session = PolarisSession(
        host="http://localhost:8181",
        client_id=cfg["CLIENT_ID"],
        client_secret=cfg["CLIENT_SECRET"],
    )
    setup_report_user(session)
    verify(session)
