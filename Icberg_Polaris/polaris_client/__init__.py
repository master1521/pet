"""
polaris_client — тонкий API-клиент к Apache Polaris 1.4.0.

Экспортирует публичные функции Management API и Catalog API.

Использование:
    from polaris_client import PolarisSession, list_catalogs
    session = PolarisSession(host="http://localhost:8181", client_id="...", client_secret="...")
    catalogs = list_catalogs(session)
"""

from polaris_client.session import PolarisSession
from polaris_client.management_api import (
    list_catalogs,
    get_catalog,
    create_catalog,
    update_catalog,
    delete_catalog,
    list_principals,
    get_principal,
    create_principal,
    update_principal,
    delete_principal,
    rotate_principal_credentials,
    reset_principal_credentials,
    list_principal_assigned_roles,
    assign_role_to_principal,
    unassign_role_from_principal,
    list_principal_roles,
    get_principal_role,
    create_principal_role,
    update_principal_role,
    delete_principal_role,
    list_principals_for_role,
    list_catalog_roles_for_principal_role,
    assign_catalog_role_to_principal_role,
    unassign_catalog_role_from_principal_role,
    list_catalog_roles,
    get_catalog_role,
    create_catalog_role,
    update_catalog_role,
    delete_catalog_role,
    list_grants,
    add_grant,
    revoke_grant,
    list_principal_roles_for_catalog_role,
)
from polaris_client.catalog_api import (
    get_catalog_config,
    list_namespaces,
    create_namespace,
    get_namespace,
    update_namespace_properties,
    drop_namespace,
    list_tables,
    create_table,
    get_table_metadata,
    commit_table,
    register_table,
    rename_table,
    drop_table,
    list_views,
    create_view,
    get_view,
    update_view,
    rename_view,
    drop_view,
    namespace_exists,
    table_exists,
    view_exists,
)

__all__ = [
    "PolarisSession",
    # Management API
    "list_catalogs", "get_catalog", "create_catalog", "update_catalog", "delete_catalog",
    "list_principals", "get_principal", "create_principal", "update_principal", "delete_principal",
    "rotate_principal_credentials", "reset_principal_credentials",
    "list_principal_assigned_roles", "assign_role_to_principal", "unassign_role_from_principal",
    "list_principal_roles", "get_principal_role", "create_principal_role", "update_principal_role",
    "delete_principal_role", "list_principals_for_role",
    "list_catalog_roles_for_principal_role", "assign_catalog_role_to_principal_role",
    "unassign_catalog_role_from_principal_role",
    "list_catalog_roles", "get_catalog_role", "create_catalog_role", "update_catalog_role",
    "delete_catalog_role", "list_grants", "add_grant", "revoke_grant",
    "list_principal_roles_for_catalog_role",
    # Catalog API
    "get_catalog_config",
    "list_namespaces", "create_namespace", "get_namespace",
    "update_namespace_properties", "drop_namespace",
    "list_tables", "create_table", "get_table_metadata", "commit_table",
    "register_table", "rename_table", "drop_table",
    "list_views", "create_view", "get_view", "update_view", "rename_view", "drop_view",
    "namespace_exists", "table_exists", "view_exists",
]
