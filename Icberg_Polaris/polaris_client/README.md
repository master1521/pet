# polaris_client
`polaris_client` — тонкий Python-клиент для Apache Polaris 1.4.0.
Модуль администрирует Polaris без UI: Catalog, RBAC, grants и Iceberg REST.

## Для чего нужен
- Управлять каталогами Polaris через Management API.
- Настраивать RBAC: users, roles, grants и связи между ними.
- Работать с namespaces, tables и views через Catalog API.
- Использовать один общий объект сессии с OAuth2 Bearer token.
- Проверять операции через интеграционный тест.

## Состав файлов
- `session.py` — `PolarisSession`, OAuth2, token cache и headers.
- `management_api.py` — Catalog, Principal, Role и grants.
- `catalog_api.py` — config, namespaces, tables и views.
- `__init__.py` — публичный экспорт функций модуля.
- `test_admin.py` — интеграционный тест для `_test_*` объектов.

## Что умеет Management API
- Catalogs: list, get, create, update, delete.
- Principals: list, get, create, update, delete, rotate/reset credentials.
- Principal Roles: list, get, create, update, delete, assign/unassign.
- Catalog Roles: list, get, create, update, delete, assign/unassign.
- Grants: list, add, revoke на уровнях catalog, namespace, table и view.

## Что умеет Catalog API
- Config: получить конфигурацию каталога.
- Namespaces: list, create, get, exists, update properties, drop.
- Tables: list, create, get metadata, commit, exists, register, rename, drop.
- Views: list, create, get, update, exists, rename, drop.

## Логика работы
- Первый аргумент всех функций: `session: PolarisSession`.
- URL строится из `session.host`.
- Авторизационные заголовки берутся из `session.headers`.
- Все HTTP-запросы используют `timeout=session.timeout`.
- Ошибки HTTP не скрываются: вызывается `response.raise_for_status()`.
- `update_*` сначала читают объект через GET и берут `entityVersion`.
- `*_exists` используют GET + 404, потому что HEAD может зависать.

## Важные особенности
- `rotate_principal_credentials` вызывается от имени самого Principal.
- Root-сессия получает 403 при ротации чужих credentials.
- `drop_view` и `drop_table(purge=True)` требуют свойства каталога
`polaris.config.drop-with-purge.enabled=true`.
- Table-level grant требует таблицу, иначе будет 404.
- Для Yandex Object Storage используется `stsUnavailable=True`.
- STS credential vending Polaris 1.4.0 не совместим с YOS в стенде.

## Проверка
Запуск: `uv run python -m polaris_client.test_admin`.
Требуется запущенный Polaris и переменные из файла `../.env`.
