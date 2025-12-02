# gateways.py
import sqlite3
from datetime import datetime
from typing import List, Optional, Tuple
from domain_models import (
    Device, DeviceStatus, DeviceType, 
    DataSource, DataSourceType,
    AnalysisResult, IoTNetwork
)


class DeviceGateway:
    """Gateway для работы с устройствами в БД"""
    
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
    
    def insert(self, device: Device, network_id: int) -> int:
        """Вставить устройство в БД"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO devices (device_name, status, type, network_id)
            VALUES (?, ?, ?, ?)
        ''', (device.device_name, device.status.value, device.type.value, network_id))
        
        device_id = cursor.lastrowid
        device.id = device_id
        
        # Сохранить связи устройства
        for connected_id in device.connections:
            cursor.execute('''
                INSERT INTO device_connections (device_id, connected_device_id)
                VALUES (?, ?)
            ''', (device_id, connected_id))
        
        self.conn.commit()
        return device_id
    
    def find_by_network(self, network_id: int) -> List[Device]:
        """Найти все устройства сети"""
        cursor = self.conn.cursor()
        
        # Получить устройства
        cursor.execute('''
            SELECT id, device_name, status, type 
            FROM devices 
            WHERE network_id = ?
        ''', (network_id,))
        
        devices = []
        for row in cursor.fetchall():
            device_id, name, status_str, type_str = row
            
            # Получить связи устройства
            cursor.execute('''
                SELECT connected_device_id 
                FROM device_connections 
                WHERE device_id = ?
            ''', (device_id,))
            
            connections = [row[0] for row in cursor.fetchall()]
            
            device = Device(
                id=device_id,
                device_name=name,
                status=DeviceStatus(status_str),
                type=DeviceType(type_str),
                connections=connections
            )
            devices.append(device)
        
        return devices


class DataSourceGateway:
    """Gateway для работы с источниками данных"""
    
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
    
    def insert(self, data_source: DataSource, network_id: int) -> int:
        """Вставить источник данных в БД"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO data_sources (datasource_name, last_update, type, network_id)
            VALUES (?, ?, ?, ?)
        ''', (
            data_source.datasource_name,
            data_source.last_update.isoformat(),
            data_source.type.value,
            network_id
        ))
        
        data_source_id = cursor.lastrowid
        data_source.id = data_source_id
        self.conn.commit()
        return data_source_id
    
    def find_by_network(self, network_id: int) -> List[DataSource]:
        """Найти все источники данных сети"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT id, datasource_name, last_update, type 
            FROM data_sources 
            WHERE network_id = ?
        ''', (network_id,))
        
        data_sources = []
        for row in cursor.fetchall():
            ds_id, name, last_update_str, type_str = row
            
            data_source = DataSource(
                id=ds_id,
                datasource_name=name,
                last_update=datetime.fromisoformat(last_update_str),
                type=DataSourceType(type_str)
            )
            data_sources.append(data_source)
        
        return data_sources


class AnalysisGateway:
    """Gateway для работы с результатами анализа"""
    
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
    
    def insert(self, analysis: AnalysisResult, network_id: int) -> int:
        """Вставить результат анализа в БД"""
        cursor = self.conn.cursor()
        
        # Вставить основной результат
        cursor.execute('''
            INSERT INTO analysis (centrality_score, date, network_id)
            VALUES (?, ?, ?)
        ''', (analysis.centrality_score, analysis.date.isoformat(), network_id))
        
        analysis_id = cursor.lastrowid
        analysis.id = analysis_id
        
        # Вставить изолированные узлы
        for node_id in analysis.isolated_nodes:
            cursor.execute('''
                INSERT INTO isolated_nodes (analysis_id, device_id)
                VALUES (?, ?)
            ''', (analysis_id, node_id))
        
        # Вставить избыточные связи
        for link in analysis.redundant_links:
            cursor.execute('''
                INSERT INTO redundant_links (analysis_id, device_id1, device_id2)
                VALUES (?, ?, ?)
            ''', (analysis_id, link[0], link[1]))
        
        self.conn.commit()
        return analysis_id
    
    def find_by_network(self, network_id: int) -> Optional[AnalysisResult]:
        """Найти последний анализ сети"""
        cursor = self.conn.cursor()
        
        # Получить основной результат
        cursor.execute('''
            SELECT id, centrality_score, date 
            FROM analysis 
            WHERE network_id = ? 
            ORDER BY date DESC 
            LIMIT 1
        ''', (network_id,))
        
        row = cursor.fetchone()
        if not row:
            return None
        
        analysis_id, centrality_score, date_str = row
        
        # Получить изолированные узлы
        cursor.execute('''
            SELECT device_id 
            FROM isolated_nodes 
            WHERE analysis_id = ?
        ''', (analysis_id,))
        
        isolated_nodes = [row[0] for row in cursor.fetchall()]
        
        # Получить избыточные связи
        cursor.execute('''
            SELECT device_id1, device_id2 
            FROM redundant_links 
            WHERE analysis_id = ?
        ''', (analysis_id,))
        
        redundant_links = [(row[0], row[1]) for row in cursor.fetchall()]
        
        return AnalysisResult(
            id=analysis_id,
            centrality_score=centrality_score,
            date=datetime.fromisoformat(date_str),
            isolated_nodes=isolated_nodes,
            redundant_links=redundant_links
        )


class IoTNetworkGateway:
    """Gateway для работы с IoT сетями"""
    
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
    
    def insert(self, network: IoTNetwork) -> int:
        """Вставить сеть в БД"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO iot_networks (description, network_name)
            VALUES (?, ?)
        ''', (network.description, network.network_name))
        
        network_id = cursor.lastrowid
        self.conn.commit()
        return network_id
    
    def find_by_id(self, network_id: int) -> Optional[IoTNetwork]:
        """Найти сеть по ID"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT id, description, network_name 
            FROM iot_networks 
            WHERE id = ?
        ''', (network_id,))
        
        row = cursor.fetchone()
        if not row:
            return None
        
        network = IoTNetwork(
            id=row[0],
            description=row[1],
            network_name=row[2]
        )
        return network