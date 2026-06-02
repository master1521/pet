"""
Management API — тонкий HTTP-клиент к Apache Polaris 1.4.0.

API: /api/catalog/v1/oauth/tokens
Документация: https://raw.githubusercontent.com/apache/polaris/refs/tags/apache-polaris-1.4.0/spec/generated/bundled-polaris-catalog-service.yaml

Возможности:
  - Аутентификация: OAuth2 client_credentials flow, кеширование токена
  - Заголовки: Authorization Bearer + Content-Type

Использование:
    from polaris_client.session import PolarisSession

    session = PolarisSession(host="http://localhost:8181", client_id="root", client_secret="secret")
    session_prod = PolarisSession(host="http://polaris:8181", client_id="root", client_secret="secret", timeout=(10.0, 120.0))
    print(session.headers)
"""

import time
from dataclasses import dataclass, field

import requests


@dataclass
class PolarisSession:
    """HTTP-сессия для аутентификации в Apache Polaris с кешированием токена.
    Args:
        host: базовый URL Polaris, например http://localhost:8181
        client_id: OAuth2 client ID (root или principal)
        client_secret: OAuth2 client secret
        realm: Polaris realm, по умолчанию "POLARIS"
        timeout: таймаут запросов в секундах; кортеж (connect, read) или единое число.
                 По умолчанию (10.0, 60.0) — 10 сек на connect, 60 сек на чтение ответа.
                 Для медленных операций на S3 увеличь read: timeout=(10.0, 120.0)
    Example: session = PolarisSession(host="http://localhost:8181", client_id="root", client_secret="secret")
    Example: session = PolarisSession(host="http://polaris:8181", client_id="root", client_secret="secret", timeout=(10.0, 120.0))
    """
    host: str
    client_id: str
    client_secret: str
    realm: str = "POLARIS"
    timeout: tuple[float, float] | float = (120.0, 360.0)
    _token: str = field(default="", init=False, repr=False)
    _expires_at: float = field(default=0.0, init=False, repr=False)

    def get_token(self) -> str:
        """Получить OAuth2 Bearer-токен. Переаутентифицируется автоматически при истечении TTL.
        Returns: str: access_token для Bearer-авторизации
        Example: token = session.get_token()
        """
        if self._token and time.monotonic() < self._expires_at:
            return self._token
        url = f"{self.host}/api/catalog/v1/oauth/tokens"
        response = requests.post(
            url,
            auth=(self.client_id, self.client_secret),
            data={"grant_type": "client_credentials", "scope": "PRINCIPAL_ROLE:ALL"},
            headers={"Polaris-Realm": self.realm},
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        self._token = data["access_token"]
        # expires_in в секундах; отнимаем 30 сек для запаса перед истечением
        self._expires_at = time.monotonic() + data.get("expires_in", 3600) - 30
        return self._token

    @property
    def headers(self) -> dict[str, str]:
        """Получить заголовки авторизации для API-запросов.
        Returns: dict[str, str]: Authorization Bearer и Content-Type
        Example: hdrs = session.headers
        """
        token = self.get_token()
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
