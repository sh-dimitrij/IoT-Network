"""
СИСТЕМНЫЕ ТЕСТЫ — «чёрный ящик», HTTP-сценарий
===============================================
Сквозной пользовательский сценарий через Flask test_client().
Внутренняя реализация скрыта — тестируется только HTTP-интерфейс.

Зависимости:
  - app.py (Flask-приложение)
  - SQLite in-memory — внешняя БД не нужна
  - Глобальный iot_service подменяется тестовым экземпляром

Запуск:
    pytest test_3_system.py -v --cov=app --cov=application_service --cov-report=term-missing
"""

import pytest
import app as flask_app_module
from application_service import IoTNetworkApplicationService


# ─── Фикстура ────────────────────────────────────────────────────────────────

@pytest.fixture
def client():
    """
    Flask test_client с изолированной тестовой БД.
    Подменяем глобальный iot_service in-memory экземпляром.
    """
    flask_app_module.app.config['TESTING'] = True
    flask_app_module.app.config['SECRET_KEY'] = 'test-secret'

    test_service = IoTNetworkApplicationService(':memory:')
    # Подменяем глобальный сервис в модуле app.py
    flask_app_module.iot_service = test_service

    with flask_app_module.app.test_client() as c:
        yield c

    test_service.close()


# ─── Вспомогательные функции ─────────────────────────────────────────────────

def do_login(client, username='admin', password='admin123'):
    return client.post('/login',
                       data={'login': username, 'password': password},
                       follow_redirects=False)

def get_admin_id():
    user = flask_app_module.iot_service.authenticate_user('admin', 'admin123')
    return user.id

def get_networks():
    uid = get_admin_id()
    return flask_app_module.iot_service.get_user_networks(uid)

def create_network_and_get_id(client, name='Тест-сеть'):
    """Создать сеть через HTTP и вернуть её ID."""
    client.post('/create_network',
                data={'network_name': name, 'description': ''},
                follow_redirects=False)
    networks = get_networks()
    assert len(networks) > 0, f"Сеть '{name}' не была создана"
    # Ищем сеть по имени
    for n in networks:
        if n['name'] == name:
            return n['id']
    return networks[0]['id']


# ══════════════════════════════════════════════════════════════════════════════
# СИСТЕМНЫЕ ТЕСТЫ С-01..С-08
# ══════════════════════════════════════════════════════════════════════════════

class TestSystemScenario:

    # ТВ С-01 — аутентификация с корректными данными
    def test_S01_login_success(self, client):
        """
        ИД:       POST /login, login='admin', password='admin123'
        ОЖ.РЕЗ.: HTTP 302; Location содержит '/dashboard'
        """
        resp = do_login(client)
        assert resp.status_code == 302
        assert 'dashboard' in resp.location

    # ТВ С-02 — аутентификация с неверным паролем
    def test_S02_login_wrong_password(self, client):
        """
        ИД:       POST /login, login='admin', password='wrong'
        ОЖ.РЕЗ.: HTTP 302; Location НЕ содержит 'dashboard'
        """
        resp = do_login(client, password='wrong')
        assert resp.status_code == 302
        assert 'dashboard' not in resp.location

    # ТВ С-03 — обращение к защищённой странице без авторизации
    def test_S03_dashboard_without_auth_redirects(self, client):
        """
        ИД:       GET /dashboard без активной сессии
        ОЖ.РЕЗ.: HTTP 302 (редирект на страницу входа)
        """
        resp = client.get('/dashboard')
        assert resp.status_code == 302

    # ТВ С-04 — создание IoT-сети
    def test_S04_create_network(self, client):
        """
        ИД:       POST /create_network, network_name='Умный офис'
        ОЖ.РЕЗ.: HTTP 302; сеть появляется в списке сетей пользователя
        """
        do_login(client)
        resp = client.post('/create_network',
                           data={'network_name': 'Умный офис', 'description': 'Тест'},
                           follow_redirects=False)
        assert resp.status_code == 302
        networks = get_networks()
        assert any(n['name'] == 'Умный офис' for n in networks)

    # ТВ С-05 — загрузка готового датасета (UC-01)
    def test_S05_load_sample_data(self, client):
        """
        ИД:       POST /load_data/{id}, action='load_sample', dataset='smart_home'
        ОЖ.РЕЗ.: HTTP 302; устройства добавлены в сеть
        """
        do_login(client)
        nid = create_network_and_get_id(client, 'Сеть-загрузка')

        resp = client.post(f'/load_data/{nid}',
                           data={'action': 'load_sample', 'dataset': 'smart_home'},
                           follow_redirects=False)
        assert resp.status_code == 302

        details = flask_app_module.iot_service.get_network_details(nid)
        assert len(details.get('devices', [])) > 0

    # ТВ С-06 — запуск анализа топологии (UC-02)
    def test_S06_analyze_topology(self, client):
        """
        ИД:       POST /analyze/{id} после загрузки данных smart_home
        ОЖ.РЕЗ.: HTTP 200 или 302; centrality_score >= 0.0 в результате
        """
        do_login(client)
        nid = create_network_and_get_id(client, 'Сеть-анализ')

        # Загружаем данные (UC-01)
        client.post(f'/load_data/{nid}',
                    data={'action': 'load_sample', 'dataset': 'smart_home'},
                    follow_redirects=False)

        # Запускаем анализ (UC-02)
        resp = client.post(f'/analyze/{nid}', follow_redirects=False)
        assert resp.status_code in (200, 302)

        # Проверяем результат через сервис — используем правильные ключи ответа
        result = flask_app_module.iot_service.analyze_topology_and_connections(nid)
        assert result['success'] is True
        assert result['centrality_score'] >= 0.0
        assert 'isolated_nodes_count' in result
        assert 'redundant_links_count' in result
        assert 'recommendations' in result

    # ТВ С-07 — удаление сети
    def test_S07_delete_network(self, client):
        """
        ИД:       POST /delete_network/{id}
        ОЖ.РЕЗ.: HTTP 302; сеть исчезает из списка сетей пользователя
        """
        do_login(client)
        nid = create_network_and_get_id(client, 'Сеть-удаление')

        resp = client.post(f'/delete_network/{nid}', follow_redirects=False)
        assert resp.status_code == 302

        networks = get_networks()
        assert not any(n['id'] == nid for n in networks)

    # ТВ С-08 — выход из системы
    def test_S08_logout(self, client):
        """
        ИД:       GET /logout после авторизации
        ОЖ.РЕЗ.: HTTP 302; последующий GET /dashboard снова редиректит на '/'
        """
        do_login(client)

        resp = client.get('/logout')
        assert resp.status_code == 302

        # После выхода — защищённая страница недоступна
        resp2 = client.get('/dashboard')
        assert resp2.status_code == 302
        assert 'dashboard' not in resp2.location