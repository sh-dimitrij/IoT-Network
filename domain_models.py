# domain_models.py
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Set, Optional, Tuple
from enum import Enum


class DeviceStatus(Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    MAINTENANCE = "maintenance"


class DeviceType(Enum):
    SENSOR = "sensor"
    ACTUATOR = "actuator"
    GATEWAY = "gateway"
    CONTROLLER = "controller"


class DataSourceType(Enum):
    API = "api"
    DATABASE = "database"
    FILE = "file"
    STREAM = "stream"


@dataclass
class Device:
    """Доменная модель устройства IoT"""
    id: int
    device_name: str
    status: DeviceStatus
    type: DeviceType
    connections: List[int]  # IDs других устройств для связей
    
    def is_active(self) -> bool:
        return self.status == DeviceStatus.ACTIVE
    
    def add_connection(self, device_id: int):
        if device_id not in self.connections and device_id != self.id:
            self.connections.append(device_id)


@dataclass
class DataSource:
    """Доменная модель источника данных"""
    id: int
    datasource_name: str
    last_update: datetime
    type: DataSourceType
    
    def needs_update(self, threshold_hours: int = 24) -> bool:
        time_since_update = datetime.now() - self.last_update
        return time_since_update.total_seconds() / 3600 > threshold_hours


@dataclass
class AnalysisResult:
    """Доменная модель результата анализа"""
    id: int
    centrality_score: float  # Средняя центральность сети
    date: datetime
    isolated_nodes: List[int]  # IDs изолированных устройств
    redundant_links: List[Tuple[int, int]]  # Избыточные связи
    
    def has_issues(self) -> bool:
        return len(self.isolated_nodes) > 0 or len(self.redundant_links) > 0
    
    def get_issue_count(self) -> int:
        return len(self.isolated_nodes) + len(self.redundant_links)


class IoTNetwork:
    """Доменная модель IoT сети с методами анализа на основе метаграфов"""
    
    def __init__(self, id: int, description: str, network_name: str):
        self.id = id
        self.description = description
        self.network_name = network_name
        self._devices: Dict[int, Device] = {}
        self._data_sources: List[DataSource] = []
        self._analysis: Optional[AnalysisResult] = None
    
    def add_device(self, device: Device):
        """Добавить устройство в сеть"""
        self._devices[device.id] = device
    
    def add_data_source(self, data_source: DataSource):
        """Добавить источник данных"""
        self._data_sources.append(data_source)
    
    def get_device(self, device_id: int) -> Optional[Device]:
        """Получить устройство по ID"""
        return self._devices.get(device_id)
    
    def get_all_devices(self) -> List[Device]:
        """Получить все устройства"""
        return list(self._devices.values())
    
    def analyze_topology(self) -> AnalysisResult:
        """
        Проанализировать топологию сети на основе метаграфов
        Возвращает результат анализа
        """
        if not self._devices:
            raise ValueError("Нет устройств для анализа")
        
        # 1. Найти изолированные узлы (без связей)
        isolated_nodes = [
            device_id for device_id, device in self._devices.items()
            if not device.connections
        ]
        
        # 2. Найти избыточные связи (дублирующиеся или симметричные)
        redundant_links = []
        seen_links: Set[Tuple[int, int]] = set()
        
        for device_id, device in self._devices.items():
            for connected_id in device.connections:
                link = tuple(sorted((device_id, connected_id)))
                
                if link in seen_links:
                    # Дублирующаяся связь
                    redundant_links.append(link)
                elif (connected_id in self._devices and 
                      device_id in self._devices[connected_id].connections):
                    # Симметричная связь (уже учтена с другой стороны)
                    redundant_links.append(link)
                else:
                    seen_links.add(link)
        
        # 3. Рассчитать центральность сети (степенная центральность)
        centrality_scores = []
        for device in self._devices.values():
            degree = len(device.connections)
            max_possible_degree = len(self._devices) - 1
            if max_possible_degree > 0:
                centrality = degree / max_possible_degree
                centrality_scores.append(centrality)
        
        avg_centrality = sum(centrality_scores) / len(centrality_scores) if centrality_scores else 0
        
        # 4. Создать результат анализа
        self._analysis = AnalysisResult(
            id=0,  # ID будет установлен при сохранении
            centrality_score=avg_centrality,
            date=datetime.now(),
            isolated_nodes=isolated_nodes,
            redundant_links=redundant_links
        )
        
        return self._analysis
    
    def get_analysis(self) -> Optional[AnalysisResult]:
        """Получить последний результат анализа"""
        return self._analysis
    
    def validate_network(self) -> bool:
        """Проверить валидность сети"""
        if not self._devices:
            return False
        
        # Проверить, что все связи ссылаются на существующие устройства
        for device in self._devices.values():
            for connected_id in device.connections:
                if connected_id not in self._devices:
                    return False
        
        return True