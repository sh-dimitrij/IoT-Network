"""
МОДУЛЬНЫЕ ТЕСТЫ — структурное тестирование («белый ящик»)
=========================================================
Метод базового пути (Мак Кейб, 1976) + тестирование условий.
Объекты: доменные модели IoTNetwork, Analysis, Device.

Зависимости: только domain_models.py — БД не нужна, объекты создаются в памяти.

Запуск:
    pytest test_1_unit.py -v
"""

import pytest
from datetime import datetime
from domain_models import (
    Device, DeviceStatus, DeviceType,
    IoTNetwork, Analysis,
)


# ─── Вспомогательная фабрика ─────────────────────────────────────────────────

def make_device(did, connections=None):
    return Device(
        id=did,
        device_name=f"Device-{did}",
        status=DeviceStatus.ACTIVE,
        type=DeviceType.SENSOR,
        network_id=1,
        connections=connections if connections is not None else []
    )

def make_analysis(centrality=0.5, isolated=None, redundant=None):
    return Analysis(
        id=1,
        centrality_score=centrality,
        date=datetime.now(),
        network_id=1,
        isolated_nodes=isolated or [],
        redundant_links=redundant or []
    )


# ═════════════════════════════════════════════════════════════════════════════
# МЕТОД БАЗОВОГО ПУТИ — IoTNetwork.analyze_topology()
# Цикломатическая сложность V(G) = 4  →  4 независимых пути: М-01..М-04
# ═════════════════════════════════════════════════════════════════════════════

class TestBasePathAnalyzeTopology:

    def setup_method(self):
        self.net = IoTNetwork(id=1, description="Test", network_name="Test")

    # ТВ М-01 | Путь 1 — пустой список → исключение
    def test_M01_empty_devices_raises_ValueError(self):
        """
        ИД:       devices = []
        ОЖ.РЕЗ.: ValueError с текстом 'Нет устройств'
        Покрытие: первая ветвь — проверка пустого списка
        """
        with pytest.raises(ValueError, match="Нет устройств"):
            self.net.analyze_topology([])

    # ТВ М-02 | Путь 2 — одно устройство без связей → изолированный узел
    def test_M02_single_isolated_device(self):
        """
        ИД:       1 устройство, connections=[]
        ОЖ.РЕЗ.: isolated_nodes=[1], redundant_links=[], centrality_score=0.0
        Покрытие: путь без связей
        """
        result = self.net.analyze_topology([make_device(1)])
        assert result.isolated_nodes == [1]
        assert result.redundant_links == []
        assert result.centrality_score == pytest.approx(0.0)

    # ТВ М-03 | Путь 3 — нормальная сеть, нет проблем
    def test_M03_normal_network_no_issues(self):
        """
        ИД:       3 устройства с корректными двусторонними связями
        ОЖ.РЕЗ.: isolated_nodes=[], 0 < centrality_score <= 1
        Покрытие: нормальный путь без аномалий
        """
        d1 = make_device(1, connections=[2])
        d2 = make_device(2, connections=[1, 3])
        d3 = make_device(3, connections=[2])
        result = self.net.analyze_topology([d1, d2, d3])
        assert result.isolated_nodes == []
        assert 0.0 < result.centrality_score <= 1.0

    # ТВ М-04 | Путь 4 — дублирующаяся связь → redundant_links не пуст
    def test_M04_redundant_links_detected(self):
        """
        ИД:       устройство 1 дважды ссылается на устройство 2
        ОЖ.РЕЗ.: redundant_links не пуст
        Покрытие: ветвь обнаружения избыточной связи
        """
        d1 = make_device(1, connections=[2, 2])
        d2 = make_device(2, connections=[1])
        result = self.net.analyze_topology([d1, d2])
        assert len(result.redundant_links) > 0


# ═════════════════════════════════════════════════════════════════════════════
# ТЕСТИРОВАНИЕ УСЛОВИЙ — Analysis.get_recommendations(), Device.add_connection()
# Для каждого условия проверяются ветви True и False: М-05..М-08
# ═════════════════════════════════════════════════════════════════════════════

class TestConditions:

    # ТВ М-05 — условие centrality < 0.3 → ветвь True
    def test_M05_low_centrality_recommendation(self):
        """
        ИД:       Analysis, centrality_score=0.1
        ОЖ.РЕЗ.: рекомендация упоминает низкую центральность
        Покрытие: ветвь True для условия centrality < 0.3
        """
        a = make_analysis(centrality=0.1)
        recs = a.get_recommendations()
        assert any("низк" in r.lower() and "центральност" in r.lower() for r in recs)

    # ТВ М-06 — условие centrality > 0.7 → ветвь True
    def test_M06_high_centrality_recommendation(self):
        """
        ИД:       Analysis, centrality_score=0.9
        ОЖ.РЕЗ.: рекомендация упоминает высокую центральность или перегрузку
        Покрытие: ветвь True для условия centrality > 0.7
        """
        a = make_analysis(centrality=0.9)
        recs = a.get_recommendations()
        assert any("высок" in r.lower() or "перегруж" in r.lower() for r in recs)

    # ТВ М-07 — условие device_id != self.id → ветвь False (петля на себя)
    def test_M07_self_loop_not_added(self):
        """
        ИД:       device.id=1, add_connection(1)
        ОЖ.РЕЗ.: connections остаётся пустым — петля не добавляется
        Покрытие: ветвь False для условия проверки самосвязи
        """
        d = make_device(1)
        d.add_connection(1)
        assert 1 not in d.connections

    # ТВ М-08 — условие device_id not in connections → ветвь False (дубликат)
    def test_M08_duplicate_connection_not_added(self):
        """
        ИД:       add_connection(2) вызывается дважды
        ОЖ.РЕЗ.: connections.count(2) == 1, дубликат не создаётся
        Покрытие: ветвь False для условия проверки дубликата
        """
        d = make_device(1)
        d.add_connection(2)
        d.add_connection(2)
        assert d.connections.count(2) == 1

    # ТВ М-09 — has_issues() при отсутствии проблем → False
    def test_M09_has_issues_false_when_clean(self):
        """
        ИД:       isolated_nodes=[], redundant_links=[]
        ОЖ.РЕЗ.: has_issues() = False
        """
        a = make_analysis()
        assert a.has_issues() is False

    # ТВ М-10 — has_issues() при наличии изолированных узлов → True
    def test_M10_has_issues_true_when_isolated(self):
        """
        ИД:       isolated_nodes=[1]
        ОЖ.РЕЗ.: has_issues() = True
        """
        a = make_analysis(isolated=[1])
        assert a.has_issues() is True