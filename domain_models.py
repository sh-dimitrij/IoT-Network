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


class UserRole(Enum):
    ADMIN = "admin"
    ANALYST = "analyst"


@dataclass
class User:
    """Доменная модель пользователя"""
    id: int
    name: str
    login: str
    password: str
    role: UserRole
    
    def is_admin(self) -> bool:
        return self.role == UserRole.ADMIN
    
    def is_analyst(self) -> bool:
        return self.role == UserRole.ANALYST
    
    def can_manage_networks(self) -> bool:
        return True  # Все пользователи могут управлять своими сетями
    
    def can_analyze_topology(self) -> bool:
        return True  # Все пользователи могут анализировать


@dataclass
class Device:
    """Доменная модель устройства IoT"""
    id: int
    device_name: str
    status: DeviceStatus
    type: DeviceType
    network_id: int
    connections: List[int]  # IDs других устройств для связей
    
    def is_active(self) -> bool:
        return self.status == DeviceStatus.ACTIVE
    
    def add_connection(self, device_id: int):
        """Добавить связь с другим устройством"""
        if device_id not in self.connections and device_id != self.id:
            self.connections.append(device_id)
    
    def remove_connection(self, device_id: int):
        """Удалить связь с другим устройством"""
        if device_id in self.connections:
            self.connections.remove(device_id)
    
    def has_connection(self, device_id: int) -> bool:
        """Проверить наличие связи с устройством"""
        return device_id in self.connections


@dataclass
class DataSource:
    """Доменная модель источника данных"""
    id: int
    datasource_name: str
    last_update: datetime
    type: DataSourceType
    network_id: int
    
    def needs_update(self, threshold_hours: int = 24) -> bool:
        """Проверить, нужно ли обновить данные"""
        time_since_update = datetime.now() - self.last_update
        return time_since_update.total_seconds() / 3600 > threshold_hours
    
    def update_timestamp(self):
        """Обновить время последнего обновления"""
        self.last_update = datetime.now()


@dataclass
class AnalysisResult:
    """Доменная модель результата анализа"""
    id: int
    centrality_score: float  # Средняя центральность сети
    date: datetime
    network_id: int
    isolated_nodes: List[int]  # IDs изолированных устройств
    redundant_links: List[Tuple[int, int]]  # Избыточные связи
    
    def has_issues(self) -> bool:
        """Есть ли проблемы в сети?"""
        return len(self.isolated_nodes) > 0 or len(self.redundant_links) > 0
    
    def get_issue_count(self) -> int:
        """Общее количество проблем"""
        return len(self.isolated_nodes) + len(self.redundant_links)
    
    def get_recommendations(self) -> List[str]:
        """Сгенерировать рекомендации на основе анализа"""
        recommendations = []
        
        if self.isolated_nodes:
            recommendations.append(
                f"Обнаружено {len(self.isolated_nodes)} изолированных устройств. "
                "Рекомендуется проверить их подключение к сети или удалить, если они не используются."
            )
        
        if self.redundant_links:
            recommendations.append(
                f"Обнаружено {len(self.redundant_links)} избыточных связей. "
                "Рекомендуется удалить дублирующиеся связи для оптимизации сети."
            )
        
        if self.centrality_score < 0.3:
            recommendations.append(
                f"Низкая центральность сети ({self.centrality_score:.2f}). "
                "Рекомендуется добавить больше связей между ключевыми устройствами."
            )
        elif self.centrality_score > 0.7:
            recommendations.append(
                f"Высокая центральность сети ({self.centrality_score:.2f}). "
                "Сеть может быть перегружена - рассмотрите возможность распределения нагрузки."
            )
        
        if not recommendations:
            recommendations.append("Сеть в хорошем состоянии. Серьезных проблем не обнаружено.")
        
        return recommendations


class IoTNetwork:
    """Доменная модель IoT сети с методами анализа на основе метаграфов"""
    
    def __init__(self, id: int, description: str, network_name: str, user_id: Optional[int] = None):
        self.id = id
        self.description = description
        self.network_name = network_name
        self.user_id = user_id
        self.created_at: Optional[datetime] = None
    
    def analyze_topology(self, devices: List[Device]) -> AnalysisResult:
        """
        Проанализировать топологию сети на основе метаграфов
        Возвращает результат анализа
        """
        if not devices:
            raise ValueError("Нет устройств для анализа")
        
        # Создаем словарь устройств для быстрого доступа
        devices_dict = {device.id: device for device in devices}
        
        # 1. Найти изолированные узлы (без связей)
        isolated_nodes = [
            device_id for device_id, device in devices_dict.items()
            if not device.connections
        ]
        
        # 2. Найти избыточные связи (дублирующиеся или симметричные)
        redundant_links = []
        seen_links: Set[Tuple[int, int]] = set()
        
        for device_id, device in devices_dict.items():
            for connected_id in device.connections:
                link = tuple(sorted((device_id, connected_id)))
                
                if link in seen_links:
                    # Дублирующаяся связь
                    redundant_links.append(link)
                elif (connected_id in devices_dict and 
                      device_id in devices_dict[connected_id].connections):
                    # Симметричная связь (уже учтена с другой стороны)
                    redundant_links.append(link)
                else:
                    seen_links.add(link)
        
        # 3. Рассчитать центральность сети (степенная центральность)
        centrality_scores = []
        for device in devices_dict.values():
            degree = len(device.connections)
            max_possible_degree = len(devices_dict) - 1
            if max_possible_degree > 0:
                centrality = degree / max_possible_degree
                centrality_scores.append(centrality)
        
        avg_centrality = sum(centrality_scores) / len(centrality_scores) if centrality_scores else 0
        
        # 4. Создать результат анализа
        return AnalysisResult(
            id=0,  # ID будет установлен при сохранении
            centrality_score=avg_centrality,
            date=datetime.now(),
            network_id=self.id,
            isolated_nodes=isolated_nodes,
            redundant_links=redundant_links
        )
    
    def validate_connections(self, devices: List[Device]) -> bool:
        """Проверить валидность всех связей в сети"""
        if not devices:
            return False
        
        device_ids = {device.id for device in devices}
        
        # Проверить, что все связи ссылаются на существующие устройства
        for device in devices:
            for connected_id in device.connections:
                if connected_id not in device_ids:
                    return False
        
        return True
    
    def get_network_summary(self, devices: List[Device], data_sources: List[DataSource]) -> Dict[str, any]:
        """Получить сводку по сети"""
        active_devices = [d for d in devices if d.is_active()]
        device_types = {}
        for device in devices:
            device_types[device.type.value] = device_types.get(device.type.value, 0) + 1
        
        total_connections = sum(len(device.connections) for device in devices)
        
        return {
            'total_devices': len(devices),
            'active_devices': len(active_devices),
            'device_types': device_types,
            'total_connections': total_connections,
            'data_sources_count': len(data_sources),
            'avg_connections_per_device': total_connections / len(devices) if devices else 0
        }