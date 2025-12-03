# gateways.py
import sqlite3
from datetime import datetime
from typing import List, Optional, Any, Dict
from domain_models import (
    User, UserRole,
    Device, DeviceStatus, DeviceType,
    DataSource, DataSourceType,
    Analysis,
    IoTNetwork
)


class UserGateway:
    """Row Data Gateway для пользователя (один объект = одна запись)"""
    
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
        self.id: Optional[int] = None
        self.name: Optional[str] = None
        self.login: Optional[str] = None
        self.password: Optional[str] = None
        self.role: Optional[str] = None
    
    def load(self, user_id: int) -> bool:
        """Загрузить данные пользователя по ID"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT id, name, login, password, role FROM users WHERE id = ?', (user_id,))
        row = cursor.fetchone()
        
        if row:
            self.id, self.name, self.login, self.password, self.role = row
            return True
        return False
    
    def insert(self) -> int:
        """Вставить текущие данные в БД"""
        if self.id is not None:
            raise ValueError("Пользователь уже имеет ID")
        
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO users (name, login, password, role)
            VALUES (?, ?, ?, ?)
        ''', (self.name, self.login, self.password, self.role))
        
        self.id = cursor.lastrowid
        self.conn.commit()
        return self.id
    
    def update(self) -> bool:
        """Обновить данные пользователя в БД"""
        if self.id is None:
            raise ValueError("Пользователь не имеет ID")
        
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE users 
            SET name = ?, login = ?, password = ?, role = ?
            WHERE id = ?
        ''', (self.name, self.login, self.password, self.role, self.id))
        
        self.conn.commit()
        return cursor.rowcount > 0
    
    def delete(self) -> bool:
        """Удалить пользователя из БД"""
        if self.id is None:
            raise ValueError("Пользователь не имеет ID")
        
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM users WHERE id = ?', (self.id,))
        
        self.conn.commit()
        return cursor.rowcount > 0
    
    def to_domain_model(self) -> Optional[User]:
        """Преобразовать в доменную модель"""
        if self.id is None:
            return None
        
        return User(
            id=self.id,
            name=self.name,
            login=self.login,
            password=self.password,
            role=UserRole(self.role)
        )
    
    def from_domain_model(self, user: User):
        """Загрузить данные из доменной модели"""
        self.id = user.id
        self.name = user.name
        self.login = user.login
        self.password = user.password
        self.role = user.role.value


class UserFinder:
    """Finder для поиска пользователей"""
    
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
    
    def find_by_id(self, user_id: int) -> Optional[UserGateway]:
        """Найти пользователя по ID"""
        gateway = UserGateway(self.conn)
        if gateway.load(user_id):
            return gateway
        return None
    
    def find_by_login(self, login: str) -> Optional[UserGateway]:
        """Найти пользователя по логину"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT id FROM users WHERE login = ?', (login,))
        row = cursor.fetchone()
        
        if row:
            gateway = UserGateway(self.conn)
            gateway.load(row[0])
            return gateway
        return None
    
    def find_all(self) -> List[UserGateway]:
        """Найти всех пользователей"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT id FROM users ORDER BY id')
        
        gateways = []
        for row in cursor.fetchall():
            gateway = UserGateway(self.conn)
            if gateway.load(row[0]):
                gateways.append(gateway)
        
        return gateways


class DeviceGateway:
    """ Row Data Gateway для устройства IoT"""
    
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
        self._clear()
    
    def _clear(self):
        """Очистить все поля объекта"""
        self.id = None
        self.device_name = None
        self.status = None
        self.type = None
        self.network_id = None
    
    def load(self, device_id: int) -> bool:
        """Загрузить данные устройства по ID"""
        self._clear()
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT id, device_name, status, type, network_id 
            FROM devices 
            WHERE id = ?
        ''', (device_id,))
        row = cursor.fetchone()
        
        if row:
            self.id, self.device_name, self.status, self.type, self.network_id = row
            return True
        return False
    
    def insert(self) -> int:
        """Вставить текущие данные в БД"""
        if self.id is not None:
            raise ValueError("Устройство уже имеет ID")
        
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO devices (device_name, status, type, network_id)
            VALUES (?, ?, ?, ?)
        ''', (self.device_name, self.status, self.type, self.network_id))
        
        self.id = cursor.lastrowid
        self.conn.commit()
        return self.id
    
    def update(self) -> bool:
        """Обновить данные устройства в БД"""
        if self.id is None:
            raise ValueError("Устройство не имеет ID")
        
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE devices 
            SET device_name = ?, status = ?, type = ?, network_id = ?
            WHERE id = ?
        ''', (self.device_name, self.status, self.type, self.network_id, self.id))
        
        self.conn.commit()
        return cursor.rowcount > 0
    
    def delete(self) -> bool:
        """Удалить устройство из БД"""
        if self.id is None:
            raise ValueError("Устройство не имеет ID")
        
        cursor = self.conn.cursor()

        cursor.execute('DELETE FROM device_connections WHERE device_id = ? OR connected_device_id = ?', 
                      (self.id, self.id))

        cursor.execute('DELETE FROM devices WHERE id = ?', (self.id,))
        self.conn.commit()

        cursor.execute('DELETE FROM isolated_nodes WHERE device_id = ?', (self.id,))
        cursor.execute('DELETE FROM redundant_links WHERE device_id1 = ? OR device_id2 = ?', 
                      (self.id, self.id))
        
        self.conn.commit()
        deleted = cursor.rowcount > 0

        self._clear()
        
        return deleted
    
    def to_domain_model(self) -> Optional[Device]:
        """Преобразовать в доменную модель"""
        if self.id is None:
            return None

        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT connected_device_id 
            FROM device_connections 
            WHERE device_id = ?
        ''', (self.id,))
        
        connections = [row[0] for row in cursor.fetchall()]
        
        return Device(
            id=self.id,
            device_name=self.device_name,
            status=DeviceStatus(self.status),
            type=DeviceType(self.type),
            network_id=self.network_id,
            connections=connections
        )
    
    def from_domain_model(self, device: Device):
        """Загрузить данные из доменной модели"""
        self.id = device.id
        self.device_name = device.device_name
        self.status = device.status.value
        self.type = device.type.value
        self.network_id = device.network_id
    
    def save_connections(self, connections: List[int]):
        """Сохранить связи устройства"""
        if self.id is None:
            raise ValueError("Устройство не имеет ID")
        
        cursor = self.conn.cursor()

        cursor.execute('DELETE FROM device_connections WHERE device_id = ?', (self.id,))

        for connected_id in connections:
            cursor.execute('''
                INSERT INTO device_connections (device_id, connected_device_id)
                VALUES (?, ?)
            ''', (self.id, connected_id))
        
        self.conn.commit()


class DeviceFinder:
    """Finder для поиска устройств"""
    
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
    
    def find_by_id(self, device_id: int) -> Optional[DeviceGateway]:
        """Найти устройство по ID"""
        gateway = DeviceGateway(self.conn)
        if gateway.load(device_id):
            return gateway
        return None
    
    def find_by_network(self, network_id: int) -> List[DeviceGateway]:
        """Найти все устройства сети"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT id FROM devices WHERE network_id = ? ORDER BY id', (network_id,))
        
        gateways = []
        for row in cursor.fetchall():
            gateway = DeviceGateway(self.conn)
            if gateway.load(row[0]):
                gateways.append(gateway)
        
        return gateways
    
    def find_active_devices(self, network_id: int) -> List[DeviceGateway]:
        """Найти активные устройства сети"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT id FROM devices 
            WHERE network_id = ? AND status = 'active' 
            ORDER BY id
        ''', (network_id,))
        
        gateways = []
        for row in cursor.fetchall():
            gateway = DeviceGateway(self.conn)
            if gateway.load(row[0]):
                gateways.append(gateway)
        
        return gateways


class IoTNetworkGateway:
    """Row Data Gateway для IoT сети"""
    
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
        self.id: Optional[int] = None
        self.description: Optional[str] = None
        self.network_name: Optional[str] = None
        self.user_id: Optional[int] = None
        self.created_at: Optional[str] = None
    
    def load(self, network_id: int) -> bool:
        """Загрузить данные сети по ID"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT id, description, network_name, user_id, created_at 
            FROM iot_networks 
            WHERE id = ?
        ''', (network_id,))
        row = cursor.fetchone()
        
        if row:
            self.id, self.description, self.network_name, self.user_id, self.created_at = row
            return True
        return False
    
    def insert(self) -> int:
        """Вставить текущие данные в БД"""
        if self.id is not None:
            raise ValueError("Сеть уже имеет ID")
        
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO iot_networks (description, network_name, user_id)
            VALUES (?, ?, ?)
        ''', (self.description, self.network_name, self.user_id))
        
        self.id = cursor.lastrowid
        self.conn.commit()
        return self.id
    
    def update(self) -> bool:
        """Обновить данные сети в БД"""
        if self.id is None:
            raise ValueError("Сеть не имеет ID")
        
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE iot_networks 
            SET description = ?, network_name = ?, user_id = ?
            WHERE id = ?
        ''', (self.description, self.network_name, self.user_id, self.id))
        
        self.conn.commit()
        return cursor.rowcount > 0
    
    def delete(self) -> bool:
        """Удалить сеть из БД"""
        if self.id is None:
            raise ValueError("Сеть не имеет ID")
        
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM iot_networks WHERE id = ?', (self.id,))
        
        self.conn.commit()
        return cursor.rowcount > 0
    
    def to_domain_model(self) -> Optional[IoTNetwork]:
        """Преобразовать в доменную модель"""
        if self.id is None:
            return None
        
        network = IoTNetwork(
            id=self.id,
            description=self.description,
            network_name=self.network_name,
            user_id=self.user_id
        )
        
        if self.created_at:
            network.created_at = datetime.fromisoformat(self.created_at)
        
        return network
    
    def from_domain_model(self, network: IoTNetwork):
        """Загрузить данные из доменной модели"""
        self.id = network.id
        self.description = network.description
        self.network_name = network.network_name
        self.user_id = network.user_id
        if network.created_at:
            self.created_at = network.created_at.isoformat()


class IoTNetworkFinder:
    """Finder для поиска IoT сетей"""
    
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
    
    def find_by_id(self, network_id: int) -> Optional[IoTNetworkGateway]:
        """Найти сеть по ID"""
        gateway = IoTNetworkGateway(self.conn)
        if gateway.load(network_id):
            return gateway
        return None
    
    def find_by_user(self, user_id: int) -> List[IoTNetworkGateway]:
        """Найти все сети пользователя"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT id FROM iot_networks WHERE user_id = ? ORDER BY created_at DESC', (user_id,))
        
        gateways = []
        for row in cursor.fetchall():
            gateway = IoTNetworkGateway(self.conn)
            if gateway.load(row[0]):
                gateways.append(gateway)
        
        return gateways
    
    def find_all(self) -> List[IoTNetworkGateway]:
        """Найти все сети"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT id FROM iot_networks ORDER BY created_at DESC')
        
        gateways = []
        for row in cursor.fetchall():
            gateway = IoTNetworkGateway(self.conn)
            if gateway.load(row[0]):
                gateways.append(gateway)
        
        return gateways


class DataSourceGateway:
    """Row Data Gateway для источника данных"""
    
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
        self.id: Optional[int] = None
        self.datasource_name: Optional[str] = None
        self.last_update: Optional[str] = None
        self.type: Optional[str] = None
        self.network_id: Optional[int] = None
    
    def load(self, datasource_id: int) -> bool:
        """Загрузить данные источника по ID"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT id, datasource_name, last_update, type, network_id 
            FROM data_sources 
            WHERE id = ?
        ''', (datasource_id,))
        row = cursor.fetchone()
        
        if row:
            self.id, self.datasource_name, self.last_update, self.type, self.network_id = row
            return True
        return False
    
    def insert(self) -> int:
        """Вставить текущие данные в БД"""
        if self.id is not None:
            raise ValueError("Источник данных уже имеет ID")
        
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO data_sources (datasource_name, last_update, type, network_id)
            VALUES (?, ?, ?, ?)
        ''', (self.datasource_name, self.last_update, self.type, self.network_id))
        
        self.id = cursor.lastrowid
        self.conn.commit()
        return self.id
    
    def update(self) -> bool:
        """Обновить данные источника в БД"""
        if self.id is None:
            raise ValueError("Источник данных не имеет ID")
        
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE data_sources 
            SET datasource_name = ?, last_update = ?, type = ?, network_id = ?
            WHERE id = ?
        ''', (self.datasource_name, self.last_update, self.type, self.network_id, self.id))
        
        self.conn.commit()
        return cursor.rowcount > 0
    
    def delete(self) -> bool:
        """Удалить источник данных из БД"""
        if self.id is None:
            raise ValueError("Источник данных не имеет ID")
        
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM data_sources WHERE id = ?', (self.id,))
        
        self.conn.commit()
        return cursor.rowcount > 0
    
    def to_domain_model(self) -> Optional[DataSource]:
        """Преобразовать в доменную модель"""
        if self.id is None:
            return None
        
        return DataSource(
            id=self.id,
            datasource_name=self.datasource_name,
            last_update=datetime.fromisoformat(self.last_update),
            type=DataSourceType(self.type),
            network_id=self.network_id
        )
    
    def from_domain_model(self, data_source: DataSource):
        """Загрузить данные из доменной модели"""
        self.id = data_source.id
        self.datasource_name = data_source.datasource_name
        self.last_update = data_source.last_update.isoformat()
        self.type = data_source.type.value
        self.network_id = data_source.network_id


class DataSourceFinder:
    """Finder для поиска источников данных"""
    
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
    
    def find_by_id(self, datasource_id: int) -> Optional[DataSourceGateway]:
        """Найти источник данных по ID"""
        gateway = DataSourceGateway(self.conn)
        if gateway.load(datasource_id):
            return gateway
        return None
    
    def find_by_network(self, network_id: int) -> List[DataSourceGateway]:
        """Найти все источники данных сети"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT id FROM data_sources WHERE network_id = ? ORDER BY id', (network_id,))
        
        gateways = []
        for row in cursor.fetchall():
            gateway = DataSourceGateway(self.conn)
            if gateway.load(row[0]):
                gateways.append(gateway)
        
        return gateways


class AnalysisGateway:
    """Row Data Gateway для результата анализа"""
    
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
        self.id: Optional[int] = None
        self.centrality_score: Optional[float] = None
        self.date: Optional[str] = None
        self.network_id: Optional[int] = None
    
    def load(self, analysis_id: int) -> bool:
        """Загрузить данные анализа по ID"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT id, centrality_score, date, network_id 
            FROM analysis 
            WHERE id = ?
        ''', (analysis_id,))
        row = cursor.fetchone()
        
        if row:
            self.id, self.centrality_score, self.date, self.network_id = row
            return True
        return False
    
    def insert(self) -> int:
        """Вставить текущие данные в БД"""
        if self.id is not None:
            raise ValueError("Анализ уже имеет ID")
        
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO analysis (centrality_score, date, network_id)
            VALUES (?, ?, ?)
        ''', (self.centrality_score, self.date, self.network_id))
        
        self.id = cursor.lastrowid
        self.conn.commit()
        return self.id
    
    def delete(self) -> bool:
        """Удалить анализ из БД"""
        if self.id is None:
            raise ValueError("Анализ не имеет ID")
        
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM analysis WHERE id = ?', (self.id,))
        
        self.conn.commit()
        return cursor.rowcount > 0
    
    def to_domain_model(self) -> Optional[Analysis]:
        """Преобразовать в доменную модель"""
        if self.id is None:
            return None

        cursor = self.conn.cursor()
        cursor.execute('SELECT device_id FROM isolated_nodes WHERE analysis_id = ?', (self.id,))
        isolated_nodes = [row[0] for row in cursor.fetchall()]

        cursor.execute('SELECT device_id1, device_id2 FROM redundant_links WHERE analysis_id = ?', (self.id,))
        redundant_links = [(row[0], row[1]) for row in cursor.fetchall()]
        
        return Analysis(
            id=self.id,
            centrality_score=self.centrality_score,
            date=datetime.fromisoformat(self.date),
            network_id=self.network_id,
            isolated_nodes=isolated_nodes,
            redundant_links=redundant_links
        )
    
    def from_domain_model(self, analysis: Analysis):
        """Загрузить данные из доменной модели"""
        self.id = analysis.id
        self.centrality_score = analysis.centrality_score
        self.date = analysis.date.isoformat()
        self.network_id = analysis.network_id
    
    def save_details(self, isolated_nodes: List[int], redundant_links: List[tuple]):
        """Сохранить детали анализа"""
        if self.id is None:
            raise ValueError("Анализ не имеет ID")
        
        cursor = self.conn.cursor()

        for node_id in isolated_nodes:
            cursor.execute('''
                INSERT INTO isolated_nodes (analysis_id, device_id)
                VALUES (?, ?)
            ''', (self.id, node_id))

        for link in redundant_links:
            cursor.execute('''
                INSERT INTO redundant_links (analysis_id, device_id1, device_id2)
                VALUES (?, ?, ?)
            ''', (self.id, link[0], link[1]))
        
        self.conn.commit()


class AnalysisFinder:
    """Finder для поиска результатов анализа"""
    
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
    
    def find_by_id(self, analysis_id: int) -> Optional[AnalysisGateway]:
        """Найти анализ по ID"""
        gateway = AnalysisGateway(self.conn)
        if gateway.load(analysis_id):
            return gateway
        return None
    
    def find_by_network(self, network_id: int, limit: int = 5) -> List[AnalysisGateway]:
        """Найти последние анализы сети"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT id FROM analysis 
            WHERE network_id = ? 
            ORDER BY date DESC 
            LIMIT ?
        ''', (network_id, limit))
        
        gateways = []
        for row in cursor.fetchall():
            gateway = AnalysisGateway(self.conn)
            if gateway.load(row[0]):
                gateways.append(gateway)
        
        return gateways
    
    def find_last_by_network(self, network_id: int) -> Optional[AnalysisGateway]:
        """Найти последний анализ сети"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT id FROM analysis 
            WHERE network_id = ? 
            ORDER BY date DESC 
            LIMIT 1
        ''', (network_id,))
        
        row = cursor.fetchone()
        if row:
            gateway = AnalysisGateway(self.conn)
            if gateway.load(row[0]):
                return gateway
        return None