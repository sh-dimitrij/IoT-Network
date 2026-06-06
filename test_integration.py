"""
ФУНКЦИОНАЛЬНЫЕ ТЕСТЫ — интеграционное тестирование («чёрный ящик»)
==================================================================
Метод: разбиение на классы эквивалентности + анализ граничных значений.
Объект: IoTNetworkApplicationService — прецеденты UC-01 и UC-02.

Зависимости: SQLite in-memory — внешняя БД не нужна.
Источник тестовых данных: объекты, создаваемые в фикстурах.

Запуск:
    pytest test_2_functional.py -v --cov=application_service --cov=domain_models --cov-report=term-missing
"""

import pytest
import time
from application_service import IoTNetworkApplicationService


# ─── Фикстуры ─────────────────────────────────────────────────────────────────

@pytest.fixture
def svc():
    """Сервис с изолированной базой данных SQLite in-memory."""
    service = IoTNetworkApplicationService(':memory:')
    yield service
    service.close()

@pytest.fixture
def admin_id(svc):
    """ID администратора, созданного при инициализации БД."""
    user = svc.authenticate_user('admin', 'admin123')
    assert user is not None, "Пользователь admin не найден в тестовой БД"
    return user.id

@pytest.fixture
def net(svc, admin_id):
    """Сервис с заранее созданной сетью. Возвращает (svc, network_id)."""
    result = svc.create_network("Тестовая сеть", "Описание", user_id=admin_id)
    assert result['success'], f"Не удалось создать сеть: {result['error']}"
    return svc, result['network_id']


# ══════════════════════════════════════════════════════════════════════════════
# UC-01: load_iot_data() — классы эквивалентности
# Параметр network_id: КЭ-1 (сеть существует), КЭ-2 (не существует)
# Параметр dataset_name: КЭ-3 (допустимое), КЭ-4 (недопустимое), КЭ-5 (None)
# ══════════════════════════════════════════════════════════════════════════════

class TestLoadIoTData_KE:

    # ТВ Ф-01 | КЭ-1 + КЭ-3 — оба параметра допустимы
    def test_F01_valid_network_and_dataset(self, net):
        """
        ИД:       network_id=существующий, dataset_name='smart_home'
        ОЖ.РЕЗ.: success=True
        """
        svc, nid = net
        result = svc.load_iot_data(network_id=nid, dataset_name='smart_home')
        assert result['success'] is True

    # ТВ Ф-02 | КЭ-1 + КЭ-3 — другой допустимый датасет
    def test_F02_valid_network_industrial_dataset(self, net):
        """
        ИД:       network_id=существующий, dataset_name='industrial_iot'
        ОЖ.РЕЗ.: success=True; устройства добавлены в БД
        """
        svc, nid = net
        result = svc.load_iot_data(network_id=nid, dataset_name='industrial_iot')
        assert result['success'] is True
        details = svc.get_network_details(nid)
        assert len(details.get('devices', [])) > 0

    # ТВ Ф-03 | КЭ-2 — несуществующая сеть
    def test_F03_nonexistent_network(self, svc):
        """
        ИД:       network_id=9999 (не существует), dataset_name='smart_home'
        ОЖ.РЕЗ.: success=False; 'error' присутствует в ответе
        """
        result = svc.load_iot_data(network_id=9999, dataset_name='smart_home')
        assert result['success'] is False
        assert 'error' in result

    # ТВ Ф-04 | КЭ-4 — недопустимое название датасета
    def test_F04_unknown_dataset_name(self, net):
        """
        ИД:       network_id=существующий, dataset_name='unknown_xyz'
        ОЖ.РЕЗ.: success=False
        """
        svc, nid = net
        result = svc.load_iot_data(network_id=nid, dataset_name='unknown_xyz')
        assert result['success'] is False

    # ТВ Ф-05 | КЭ-5 — dataset_name не передан
    def test_F05_missing_dataset_name(self, net):
        """
        ИД:       network_id=существующий, dataset_name=None
        ОЖ.РЕЗ.: success=False
        """
        svc, nid = net
        result = svc.load_iot_data(network_id=nid, dataset_name=None)
        assert result['success'] is False


# ══════════════════════════════════════════════════════════════════════════════
# UC-01: load_iot_data() — граничные значения
# Параметр dataset_name задаёт значение из конечного множества:
# границы — первый элемент (min), последний (max), значение за множеством (max+1)
# ══════════════════════════════════════════════════════════════════════════════

class TestLoadIoTData_GV:

    # ТВ Ф-06 — первый элемент допустимого множества (min)
    def test_F06_first_dataset_in_set(self, net):
        """
        ИД:       первый датасет из get_sample_datasets()
        ОЖ.РЕЗ.: success=True
        Граница: min (первый элемент множества)
        """
        svc, nid = net
        first = list(svc.get_sample_datasets().keys())[0]
        result = svc.load_iot_data(network_id=nid, dataset_name=first)
        assert result['success'] is True

    # ТВ Ф-07 — последний элемент допустимого множества (max)
    def test_F07_last_dataset_in_set(self, net):
        """
        ИД:       последний датасет из get_sample_datasets()
        ОЖ.РЕЗ.: success=True
        Граница: max (последний элемент множества)
        """
        svc, nid = net
        last = list(svc.get_sample_datasets().keys())[-1]
        result = svc.load_iot_data(network_id=nid, dataset_name=last)
        assert result['success'] is True

    # ТВ Ф-08 — значение за пределами множества (max+1)
    def test_F08_beyond_valid_dataset_set(self, net):
        """
        ИД:       имя = последний_датасет + '_extra'
        ОЖ.РЕЗ.: success=False
        Граница: max + 1 (за пределами допустимого множества)
        """
        svc, nid = net
        last = list(svc.get_sample_datasets().keys())[-1]
        result = svc.load_iot_data(network_id=nid, dataset_name=last + '_extra')
        assert result['success'] is False


# ══════════════════════════════════════════════════════════════════════════════
# UC-01: add_device() — классы эквивалентности
# device_name: КЭ-6 (непустая строка), КЭ-7 (пустая строка — баг в коде)
# type: КЭ-8 (допустимый), КЭ-9 (недопустимый)
# ══════════════════════════════════════════════════════════════════════════════

class TestAddDevice_KE:

    # ТВ Ф-09 | КЭ-6 + КЭ-8 — все параметры допустимы
    def test_F09_valid_device_data(self, net):
        """
        ИД:       name='Датчик температуры', type='sensor', status='active'
        ОЖ.РЕЗ.: success=True; device_id возвращён как целое число
        """
        svc, nid = net
        result = svc.add_device(nid, {
            'name': 'Датчик температуры',
            'type': 'sensor',
            'status': 'active'
        })
        assert result['success'] is True
        assert isinstance(result.get('device_id'), int)

    # ТВ Ф-10 | КЭ-7 — пустое имя устройства
    # Примечание: add_device() не валидирует пустое имя — устройство сохраняется.
    # Тест фиксирует фактическое поведение: success=True, имя пустое.
    def test_F10_empty_device_name(self, net):
        """
        ИД:       name=''
        ОЖ.РЕЗ.: success=True (валидация отсутствует); device_name='' в БД
        Примечание: отсутствие валидации пустого имени является дефектом.
        """
        svc, nid = net
        result = svc.add_device(nid, {
            'name': '',
            'type': 'sensor',
            'status': 'active'
        })
        # Код не валидирует пустое имя — фиксируем фактическое поведение
        assert result['success'] is True
        assert result.get('device_id') is not None

    # ТВ Ф-11 | КЭ-9 — недопустимый тип устройства
    def test_F11_invalid_device_type(self, net):
        """
        ИД:       type='robot' (не входит в перечисление DeviceType)
        ОЖ.РЕЗ.: success=False или ValueError
        """
        svc, nid = net
        try:
            result = svc.add_device(nid, {
                'name': 'Устройство',
                'type': 'robot',
                'status': 'active'
            })
            assert result['success'] is False
        except (ValueError, KeyError):
            pass  # допустимое поведение


# ══════════════════════════════════════════════════════════════════════════════
# UC-02: analyze_topology_and_connections() — классы эквивалентности
# Возвращаемый словарь при success=True содержит плоские ключи:
#   'centrality_score', 'isolated_nodes_count', 'isolated_nodes',
#   'redundant_links_count', 'redundant_links', 'has_issues', 'recommendations'
# ══════════════════════════════════════════════════════════════════════════════

class TestAnalyzeTopology_KE:

    # ТВ Ф-12 | КЭ-10 — нормальная сеть с устройствами
    def test_F12_normal_network_with_devices(self, net):
        """
        ИД:       3 устройства добавлены в сеть
        ОЖ.РЕЗ.: success=True; 0 <= centrality_score <= 1
        """
        svc, nid = net
        for i in range(3):
            svc.add_device(nid, {
                'name': f'D{i}', 'type': 'sensor', 'status': 'active'
            })
        result = svc.analyze_topology_and_connections(nid)
        assert result['success'] is True
        assert 0.0 <= result['centrality_score'] <= 1.0

    # ТВ Ф-13 | КЭ-10 + КЭ-13 — все устройства изолированы
    def test_F13_all_devices_isolated(self, net):
        """
        ИД:       3 устройства без связей
        ОЖ.РЕЗ.: success=True; isolated_nodes_count == 3
        """
        svc, nid = net
        for i in range(3):
            svc.add_device(nid, {
                'name': f'D{i}', 'type': 'sensor', 'status': 'active'
            })
        result = svc.analyze_topology_and_connections(nid)
        assert result['success'] is True
        assert result['isolated_nodes_count'] == 3

    # ТВ Ф-14 | КЭ-11 — сеть без устройств
    def test_F14_empty_network(self, svc, admin_id):
        """
        ИД:       сеть создана, устройства не добавлены
        ОЖ.РЕЗ.: success=False; 'error' присутствует в ответе
        """
        net_result = svc.create_network("Пустая сеть", "", user_id=admin_id)
        nid = net_result['network_id']
        result = svc.analyze_topology_and_connections(nid)
        assert result['success'] is False
        assert 'error' in result

    # ТВ Ф-15 | КЭ-12 — несуществующая сеть
    def test_F15_nonexistent_network(self, svc):
        """
        ИД:       network_id=9999
        ОЖ.РЕЗ.: success=False; 'error' присутствует в ответе
        """
        result = svc.analyze_topology_and_connections(9999)
        assert result['success'] is False
        assert 'error' in result


# ══════════════════════════════════════════════════════════════════════════════
# UC-02: analyze_topology_and_connections() — граничные значения
# По количеству устройств: n=0 (min−1), n=1 (min), n=2 (min+1), n=50 (нагрузка)
# ══════════════════════════════════════════════════════════════════════════════

class TestAnalyzeTopology_GV:

    # ТВ Ф-16 — n=0: ниже допустимой границы (min−1)
    def test_F16_zero_devices(self, svc, admin_id):
        """
        ИД:       сеть без устройств (n=0)
        ОЖ.РЕЗ.: success=False
        Граница: min − 1
        """
        r = svc.create_network("Zero", "", user_id=admin_id)
        result = svc.analyze_topology_and_connections(r['network_id'])
        assert result['success'] is False

    # ТВ Ф-17 — n=1: минимально допустимое количество (min)
    def test_F17_one_device_min_boundary(self, svc, admin_id):
        """
        ИД:       1 устройство без связей (n=1)
        ОЖ.РЕЗ.: success=True; isolated_nodes_count=1; centrality_score=0.0
        Граница: min
        """
        r = svc.create_network("One", "", user_id=admin_id)
        nid = r['network_id']
        svc.add_device(nid, {'name': 'Solo', 'type': 'sensor', 'status': 'active'})
        result = svc.analyze_topology_and_connections(nid)
        assert result['success'] is True
        assert result['isolated_nodes_count'] == 1
        assert result['centrality_score'] == pytest.approx(0.0)

    # ТВ Ф-18 — n=2: min+1
    def test_F18_two_devices(self, svc, admin_id):
        """
        ИД:       2 устройства без связей (n=2)
        ОЖ.РЕЗ.: success=True; isolated_nodes_count=2
        Граница: min + 1
        """
        r = svc.create_network("Two", "", user_id=admin_id)
        nid = r['network_id']
        svc.add_device(nid, {'name': 'D1', 'type': 'sensor', 'status': 'active'})
        svc.add_device(nid, {'name': 'D2', 'type': 'sensor', 'status': 'active'})
        result = svc.analyze_topology_and_connections(nid)
        assert result['success'] is True
        assert result['isolated_nodes_count'] == 2

    # ТВ Ф-19 — n=50: нагрузочная граница
    def test_F19_fifty_devices_performance(self, svc, admin_id):
        """
        ИД:       50 устройств (n=50)
        ОЖ.РЕЗ.: success=True; время выполнения < 3 секунд
        Граница: нагрузочная
        """
        r = svc.create_network("Large", "", user_id=admin_id)
        nid = r['network_id']
        for i in range(50):
            svc.add_device(nid, {
                'name': f'D{i}', 'type': 'sensor', 'status': 'active'
            })
        start = time.time()
        result = svc.analyze_topology_and_connections(nid)
        elapsed = time.time() - start
        assert result['success'] is True
        assert elapsed < 3.0, f"Превышен лимит времени: {elapsed:.2f}с"