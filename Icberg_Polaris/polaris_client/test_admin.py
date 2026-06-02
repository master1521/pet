"""
test_admin.py — интеграционный тест всех функций polaris_client.

Запуск: uv run python -m polaris_client.test_admin

Фазы:
  1. Setup: создаёт тестовую структуру (_test_* объекты)
  2. Test: вызывает все ~70 функций, фиксирует [✓]/[✗]
  3. Teardown: удаляет все тестовые объекты (гарантированно в try/finally)

Требует: переменные окружения CLIENT_ID, CLIENT_SECRET (или .env на уровень выше)
"""

import logging
import sys
import time
from pathlib import Path

from dotenv import dotenv_values

from polaris_client.session import PolarisSession
from polaris_client import (
    # Management API
    list_catalogs, get_catalog, create_catalog, update_catalog, delete_catalog,
    list_principals, get_principal, create_principal, update_principal, delete_principal,
    rotate_principal_credentials, reset_principal_credentials,
    list_principal_assigned_roles, assign_role_to_principal, unassign_role_from_principal,
    list_principal_roles, get_principal_role, create_principal_role, update_principal_role,
    delete_principal_role, list_principals_for_role,
    list_catalog_roles_for_principal_role, assign_catalog_role_to_principal_role,
    unassign_catalog_role_from_principal_role,
    list_catalog_roles, get_catalog_role, create_catalog_role, update_catalog_role,
    delete_catalog_role, list_grants, add_grant, revoke_grant,
    list_principal_roles_for_catalog_role,
    # Catalog API
    get_catalog_config,
    list_namespaces, create_namespace, get_namespace,
    update_namespace_properties, drop_namespace,
    list_tables, create_table, get_table_metadata, commit_table,
    register_table, rename_table, drop_table,
    list_views, create_view, get_view, update_view, rename_view, drop_view,
    namespace_exists, table_exists, view_exists,
)

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)

_TEST_CATALOG = "_test_catalog"
_TEST_NS = "_test_ns"
_TEST_TABLE = "_test_table"
_TEST_TABLE_RENAMED = "_test_table_renamed"
_TEST_TABLE_REG = "_test_table_reg"
_TEST_VIEW = "_test_view"
_TEST_VIEW_RENAMED = "_test_view_renamed"
_TEST_PRINCIPAL = "_test_user"
_TEST_PR = "_test_principal_role"
_TEST_CR = "_test_catalog_role"

_ENV_PATH = Path(__file__).parent.parent.parent / ".env"


def _load_session() -> PolarisSession:
    """Создать сессию из переменных окружения.
    Returns: PolarisSession: авторизованная сессия
    Example: session = _load_session()
    """
    config = dotenv_values(_ENV_PATH)
    return PolarisSession(
        host="http://localhost:8181",
        client_id=config["CLIENT_ID"],
        client_secret=config["CLIENT_SECRET"],
    )


def setup(session: PolarisSession) -> None:
    """Создать тестовую структуру в Polaris.
    Args:
        session: активная сессия Polaris
    Returns: None
    Example: setup(session)
    """
    config = dotenv_values(_ENV_PATH)
    storage_location = f"{config['STORAGE_LOCATION']}/_test/"
    s3_endpoint = config["AWS_ENDPOINT_URL"]
    s3_access_key = config["AWS_ACCESS_KEY_ID"]
    s3_secret_key = config["AWS_SECRET_ACCESS_KEY"]

    print("\n[SETUP] Создаём тестовые объекты...")

    create_catalog(
        session,
        name=_TEST_CATALOG,
        storage_location=storage_location,
        s3_endpoint=s3_endpoint,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
        sts_unavailable=True,
    )
    # drop-with-purge должен быть включён на уровне каталога, иначе drop_view → 403
    # (Polaris 1.4.0 всегда делает purge для views; для tables аналогично, если purge=True)
    update_catalog(session, _TEST_CATALOG, properties={"polaris.config.drop-with-purge.enabled": "true"})
    create_namespace(session, _TEST_CATALOG, _TEST_NS)
    create_principal(session, _TEST_PRINCIPAL)
    create_principal_role(session, _TEST_PR)
    create_catalog_role(session, _TEST_CATALOG, _TEST_CR)
    add_grant(session, _TEST_CATALOG, _TEST_CR, "TABLE_READ_DATA", grant_type="catalog")
    assign_catalog_role_to_principal_role(session, _TEST_PR, _TEST_CATALOG, _TEST_CR)
    assign_role_to_principal(session, _TEST_PRINCIPAL, _TEST_PR)

    print("[SETUP] Готово.")


_NOT_SET = object()


def _run(
    results: dict[str, bool],
    name: str,
    fn,
    *args,
    skip: bool = False,
    expected: object = _NOT_SET,
    **kwargs,
) -> object:
    """Вызвать функцию, записать результат [✓]/[✗]/[SKIP].
    Args:
        results: словарь результатов тестов
        name: имя теста
        fn: вызываемая функция
        skip: True — пропустить без вызова
        expected: если задано, сравнить результат с этим значением и пометить [✗] при расхождении
    Returns: object: результат вызова или None при ошибке/skip
    Example: _run(results, "list_catalogs", list_catalogs, session)
    Example: _run(results, "namespace_exists (True)", namespace_exists, session, "lakehouse", "bronze", expected=True)
    """
    if skip:
        print(f"  [SKIP] {name}")
        return None
    try:
        result = fn(*args, **kwargs)
        # Сравнение с ожидаемым значением — точная проверка, а не «функция не упала»
        if expected is not _NOT_SET and result != expected:
            print(f"  [✗] {name:<50} → ожидалось {expected!r}, получено {result!r}")
            results[name] = False
            return result
        hint = ""
        if isinstance(result, list):
            hint = f"→ {len(result)} элементов"
        elif isinstance(result, dict):
            name_val = result.get("name") or result.get("catalog", {}).get("name", "")
            hint = f"→ {name_val}" if name_val else "→ ok"
        elif isinstance(result, bool):
            hint = f"→ {result}"
        elif result is None:
            hint = "→ None"
        print(f"  [✓] {name:<50} {hint}")
        results[name] = True
        return result
    except Exception as exc:
        print(f"  [✗] {name:<50} → {type(exc).__name__}: {exc}")
        results[name] = False
        return None


def run_tests(session: PolarisSession) -> dict[str, bool]:
    """Вызвать все функции polaris_client и вернуть результаты.
    Args:
        session: активная сессия Polaris
    Returns: dict[str, bool]: имя функции → True если прошло, False если нет
    Example: results = run_tests(session)
    """
    results: dict[str, bool] = {}
    print("\n[TEST] Запуск тестов...\n")

    # ── Management API: Catalogs ──────────────────────────────────────────────
    print("── Management API: Catalogs ──")
    _run(results, "list_catalogs", list_catalogs, session)
    _run(results, "get_catalog", get_catalog, session, _TEST_CATALOG)
    _run(results, "update_catalog", update_catalog, session, _TEST_CATALOG, properties={"comment": "test"})
    # create_catalog / delete_catalog тестируются косвенно в setup/teardown

    # ── Management API: Principals ────────────────────────────────────────────
    print("\n── Management API: Principals ──")
    _run(results, "list_principals", list_principals, session)
    _run(results, "get_principal", get_principal, session, _TEST_PRINCIPAL)
    _run(results, "update_principal", update_principal, session, _TEST_PRINCIPAL, properties={"comment": "test"})
    # reset под root — нужен чтобы получить актуальные credentials _test_user
    reset_resp = _run(results, "reset_principal_credentials", reset_principal_credentials, session, _TEST_PRINCIPAL)
    # rotate требует вызова от имени самого principal — создаём вторую сессию его credentials
    user_creds = reset_resp.get("credentials", {}) if isinstance(reset_resp, dict) else {}
    user_session = PolarisSession(
        host=session.host,
        client_id=user_creds.get("clientId", ""),
        client_secret=user_creds.get("clientSecret", ""),
    )
    rotate_resp = _run(results, "rotate_principal_credentials (user session)", rotate_principal_credentials, user_session, _TEST_PRINCIPAL)
    # Проверка: после rotate clientSecret сменился
    if isinstance(rotate_resp, dict):
        new_secret = rotate_resp.get("credentials", {}).get("clientSecret", "")
        if new_secret and new_secret != user_creds.get("clientSecret"):
            print(f"  [✓] rotate сменил clientSecret                          → новый секрет")
            results["rotate сменил clientSecret"] = True
        else:
            print(f"  [✗] rotate сменил clientSecret                          → секрет не сменился")
            results["rotate сменил clientSecret"] = False
    _run(results, "list_principal_assigned_roles", list_principal_assigned_roles, session, _TEST_PRINCIPAL)

    # ── Management API: Principal Roles ───────────────────────────────────────
    print("\n── Management API: Principal Roles ──")
    _run(results, "list_principal_roles", list_principal_roles, session)
    _run(results, "get_principal_role", get_principal_role, session, _TEST_PR)
    _run(results, "update_principal_role", update_principal_role, session, _TEST_PR, properties={"comment": "test"})
    _run(results, "list_principals_for_role", list_principals_for_role, session, _TEST_PR)
    _run(results, "list_catalog_roles_for_principal_role", list_catalog_roles_for_principal_role, session, _TEST_PR, _TEST_CATALOG)

    # ── Management API: Catalog Roles ─────────────────────────────────────────
    print("\n── Management API: Catalog Roles ──")
    _run(results, "list_catalog_roles", list_catalog_roles, session, _TEST_CATALOG)
    _run(results, "get_catalog_role", get_catalog_role, session, _TEST_CATALOG, _TEST_CR)
    _run(results, "update_catalog_role", update_catalog_role, session, _TEST_CATALOG, _TEST_CR, properties={"comment": "test"})
    _run(results, "list_principal_roles_for_catalog_role", list_principal_roles_for_catalog_role, session, _TEST_CATALOG, _TEST_CR)

    # ── Management API: Grants ────────────────────────────────────────────────
    print("\n── Management API: Grants ──")
    _run(results, "list_grants", list_grants, session, _TEST_CATALOG, _TEST_CR)
    _run(results, "add_grant (TABLE_LIST)", add_grant, session, _TEST_CATALOG, _TEST_CR, "TABLE_LIST", grant_type="catalog")
    _run(results, "revoke_grant (TABLE_LIST)", revoke_grant, session, _TEST_CATALOG, _TEST_CR, "TABLE_LIST", grant_type="catalog")

    # ── Catalog API: Config ───────────────────────────────────────────────────
    print("\n── Catalog API: Config ──")
    _run(results, "get_catalog_config", get_catalog_config, session, _TEST_CATALOG)

    # ── Catalog API: Namespaces ───────────────────────────────────────────────
    print("\n── Catalog API: Namespaces ──")
    _run(results, "list_namespaces", list_namespaces, session, _TEST_CATALOG)
    _run(results, "get_namespace", get_namespace, session, _TEST_CATALOG, _TEST_NS)
    _run(results, "namespace_exists (True)", namespace_exists, session, _TEST_CATALOG, _TEST_NS, expected=True)
    _run(results, "namespace_exists (False)", namespace_exists, session, _TEST_CATALOG, "_nonexistent_ns", expected=False)
    _run(results, "update_namespace_properties", update_namespace_properties, session, _TEST_CATALOG, _TEST_NS, updates={"owner": "test"})

    # ── Catalog API: Tables ───────────────────────────────────────────────────
    print("\n── Catalog API: Tables ──")
    _iceberg_schema = {
        "type": "struct",
        "schema-id": 0,
        "fields": [
            {"id": 1, "name": "id", "required": True, "type": "long"},
            {"id": 2, "name": "value", "required": False, "type": "string"},
        ],
    }
    _run(results, "create_table", create_table, session, _TEST_CATALOG, _TEST_NS, _TEST_TABLE, schema=_iceberg_schema)
    _run(results, "list_tables", list_tables, session, _TEST_CATALOG, _TEST_NS)
    _run(results, "get_table_metadata", get_table_metadata, session, _TEST_CATALOG, _TEST_NS, _TEST_TABLE)
    _run(results, "table_exists (True)", table_exists, session, _TEST_CATALOG, _TEST_NS, _TEST_TABLE, expected=True)
    _run(results, "table_exists (False)", table_exists, session, _TEST_CATALOG, _TEST_NS, "_nonexistent_table", expected=False)
    # Точечный тест commit_table: меняем properties таблицы через CommitTableRequest
    _run(
        results,
        "commit_table (set-properties)",
        commit_table,
        session,
        _TEST_CATALOG,
        _TEST_NS,
        _TEST_TABLE,
        requirements=[],
        updates=[{"action": "set-properties", "updates": {"_test_prop": "_test_value"}}],
    )
    # rename вперёд и обратно — чтобы teardown drop_table нашёл _TEST_TABLE
    _run(results, "rename_table (forward)", rename_table, session, _TEST_CATALOG, _TEST_NS, _TEST_TABLE, _TEST_TABLE_RENAMED)
    _run(results, "table_exists после rename", table_exists, session, _TEST_CATALOG, _TEST_NS, _TEST_TABLE_RENAMED, expected=True)
    _run(results, "rename_table (back)", rename_table, session, _TEST_CATALOG, _TEST_NS, _TEST_TABLE_RENAMED, _TEST_TABLE)
    # register_table: создаём отдельную, сохраняем metadata-location, дропаем без purge, регистрируем заново
    reg_meta = _run(
        results,
        "create_table (для register)",
        create_table,
        session,
        _TEST_CATALOG,
        _TEST_NS,
        _TEST_TABLE_REG,
        schema=_iceberg_schema,
    )
    metadata_location = reg_meta["metadata-location"] if isinstance(reg_meta, dict) else ""
    _run(results, "drop_table (для register, без purge)", drop_table, session, _TEST_CATALOG, _TEST_NS, _TEST_TABLE_REG, purge=False)
    _run(
        results,
        "register_table",
        register_table,
        session,
        _TEST_CATALOG,
        _TEST_NS,
        _TEST_TABLE_REG,
        metadata_location=metadata_location,
    )
    _run(results, "table_exists после register", table_exists, session, _TEST_CATALOG, _TEST_NS, _TEST_TABLE_REG, expected=True)

    # ── Catalog API: Views ────────────────────────────────────────────────────
    print("\n── Catalog API: Views ──")
    _view_schema = {
        "type": "struct",
        "schema-id": 0,
        "fields": [
            {"id": 1, "name": "id", "required": True, "type": "long"},
        ],
    }
    _view_version = {
        "version-id": 1,
        "schema-id": 0,
        "timestamp-ms": int(time.time() * 1000),
        "summary": {"operation": "create"},
        "default-namespace": [_TEST_NS],
        "representations": [
            {"type": "sql", "sql": f"SELECT id FROM {_TEST_NS}.{_TEST_TABLE}", "dialect": "spark"},
        ],
    }
    _run(results, "list_views (пусто)", list_views, session, _TEST_CATALOG, _TEST_NS)
    _run(results, "view_exists (False)", view_exists, session, _TEST_CATALOG, _TEST_NS, "_nonexistent_view", expected=False)
    _run(
        results,
        "create_view",
        create_view,
        session,
        _TEST_CATALOG,
        _TEST_NS,
        _TEST_VIEW,
        view_version=_view_version,
        schema=_view_schema,
    )
    _run(results, "view_exists (True)", view_exists, session, _TEST_CATALOG, _TEST_NS, _TEST_VIEW, expected=True)
    _run(results, "get_view", get_view, session, _TEST_CATALOG, _TEST_NS, _TEST_VIEW)
    _run(
        results,
        "update_view (set-properties)",
        update_view,
        session,
        _TEST_CATALOG,
        _TEST_NS,
        _TEST_VIEW,
        requirements=[],
        updates=[{"action": "set-properties", "updates": {"_test_prop": "_test_value"}}],
    )
    _run(results, "rename_view (forward)", rename_view, session, _TEST_CATALOG, _TEST_NS, _TEST_VIEW, _TEST_VIEW_RENAMED)
    _run(results, "view_exists после rename", view_exists, session, _TEST_CATALOG, _TEST_NS, _TEST_VIEW_RENAMED, expected=True)
    _run(results, "rename_view (back)", rename_view, session, _TEST_CATALOG, _TEST_NS, _TEST_VIEW_RENAMED, _TEST_VIEW)

    return results


def teardown(session: PolarisSession) -> None:
    """Удалить все тестовые объекты в обратном порядке.
    Args:
        session: активная сессия Polaris
    Returns: None
    Example: teardown(session)
    """
    print("\n[TEARDOWN] Удаляем тестовые объекты...")

    def _safe(name: str, fn, *args, **kwargs) -> None:
        try:
            fn(*args, **kwargs)
            print(f"  [✓] {name}")
        except Exception as exc:
            print(f"  [!] {name} → {exc}")

    _safe("drop_view", drop_view, session, _TEST_CATALOG, _TEST_NS, _TEST_VIEW)
    _safe("drop_table (_TEST_TABLE_REG)", drop_table, session, _TEST_CATALOG, _TEST_NS, _TEST_TABLE_REG, purge=False)
    _safe("drop_table", drop_table, session, _TEST_CATALOG, _TEST_NS, _TEST_TABLE, purge=False)
    _safe("unassign_catalog_role_from_principal_role", unassign_catalog_role_from_principal_role, session, _TEST_PR, _TEST_CATALOG, _TEST_CR)
    _safe("unassign_role_from_principal", unassign_role_from_principal, session, _TEST_PRINCIPAL, _TEST_PR)
    _safe("revoke_grant (TABLE_READ_DATA)", revoke_grant, session, _TEST_CATALOG, _TEST_CR, "TABLE_READ_DATA", grant_type="catalog")
    _safe("delete_catalog_role", delete_catalog_role, session, _TEST_CATALOG, _TEST_CR)
    _safe("delete_principal_role", delete_principal_role, session, _TEST_PR)
    _safe("delete_principal", delete_principal, session, _TEST_PRINCIPAL)
    _safe("drop_namespace", drop_namespace, session, _TEST_CATALOG, _TEST_NS)
    _safe("delete_catalog", delete_catalog, session, _TEST_CATALOG)

    print("[TEARDOWN] Готово.")


def print_results(results: dict[str, bool]) -> None:
    """Вывести итоговую статистику тестов.
    Args:
        results: словарь имя→bool
    Returns: None
    Example: print_results(results)
    """
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    failed_names = [k for k, v in results.items() if not v]
    print(f"\n{'=' * 60}")
    print(f"  Итог: {passed}/{total} прошло")
    if failed_names:
        print("  Не прошли:")
        for name in failed_names:
            print(f"    [✗] {name}")
    print("=" * 60)


def main() -> None:
    """Точка входа: setup → test → teardown → print_results.
    Args: None
    Returns: None
    Example: main()
    """
    session = _load_session()
    setup(session)
    results: dict[str, bool] = {}
    try:
        results = run_tests(session)
    finally:
        teardown(session)
    print_results(results)


if __name__ == "__main__":
    main()
