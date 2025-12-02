# application_service.py
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from domain_models import (
    User, UserRole,
    Device, DeviceStatus, DeviceType,
    DataSource, DataSourceType,
    IoTNetwork, AnalysisResult
)
from gateways import (
    UserFinder, UserGateway,
    DeviceFinder, DeviceGateway,
    IoTNetworkFinder, IoTNetworkGateway,
    DataSourceFinder, DataSourceGateway,
    AnalysisFinder, AnalysisGateway
)


class IoTNetworkApplicationService:
    """
    Сервисный слой для координации между Domain Model и Row Data Gateway
    Реализует прецеденты использования
    """
    
    def __init__(self, db_path: str = 'iot_network.db'):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_database()
    
    def _init_database(self):
        """Инициализировать структуру БД"""
        cursor = self.conn.cursor()
        
        # Таблица пользователей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                login TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('admin', 'analyst'))
            )
        ''')
        
        # Таблица IoT сетей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS iot_networks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                description TEXT,
                network_name TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                user_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        ''')
        
        # Таблица устройств
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS devices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_name TEXT NOT NULL,
                status TEXT NOT NULL,
                type TEXT NOT NULL,
                network_id INTEGER NOT NULL,
                FOREIGN KEY (network_id) REFERENCES iot_networks (id) ON DELETE CASCADE
            )
        ''')
        
        # Таблица связей между устройствами
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS device_connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id INTEGER NOT NULL,
                connected_device_id INTEGER NOT NULL,
                FOREIGN KEY (device_id) REFERENCES devices (id) ON DELETE CASCADE
            )
        ''')
        
        # Таблица источников данных
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                datasource_name TEXT NOT NULL,
                last_update TEXT NOT NULL,
                type TEXT NOT NULL,
                network_id INTEGER NOT NULL,
                FOREIGN KEY (network_id) REFERENCES iot_networks (id) ON DELETE CASCADE
            )
        ''')
        
        # Таблица результатов анализа
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                centrality_score REAL NOT NULL,
                date TEXT NOT NULL,
                network_id INTEGER NOT NULL,
                FOREIGN KEY (network_id) REFERENCES iot_networks (id) ON DELETE CASCADE
            )
        ''')
        
        # Таблица изолированных узлов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS isolated_nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                analysis_id INTEGER NOT NULL,
                device_id INTEGER NOT NULL,
                FOREIGN KEY (analysis_id) REFERENCES analysis (id) ON DELETE CASCADE,
                FOREIGN KEY (device_id) REFERENCES devices (id) ON DELETE CASCADE
            )
        ''')
        
        # Таблица избыточных связей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS redundant_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                analysis_id INTEGER NOT NULL,
                device_id1 INTEGER NOT NULL,
                device_id2 INTEGER NOT NULL,
                FOREIGN KEY (analysis_id) REFERENCES analysis (id) ON DELETE CASCADE,
                FOREIGN KEY (device_id1) REFERENCES devices (id) ON DELETE CASCADE,
                FOREIGN KEY (device_id2) REFERENCES devices (id) ON DELETE CASCADE
            )
        ''')
        
        # Создаем тестовых пользователей, если нет
        user_finder = UserFinder(self.conn)
        if not user_finder.find_by_login('admin'):
            admin_gateway = UserGateway(self.conn)
            admin_gateway.name = 'Администратор'
            admin_gateway.login = 'admin'
            admin_gateway.password = 'admin123'
            admin_gateway.role = 'admin'
            admin_gateway.insert()
        
        if not user_finder.find_by_login('analyst'):
            analyst_gateway = UserGateway(self.conn)
            analyst_gateway.name = 'Аналитик'
            analyst_gateway.login = 'analyst'
            analyst_gateway.password = 'analyst123'
            analyst_gateway.role = 'analyst'
            analyst_gateway.insert()
        
        self.conn.commit()
    
    # ===== ПРЕЦЕДЕНТ 1: ЗАГРУЗКА ДАННЫХ ОБ УСТРОЙСТВАХ И СВЯЗЯХ =====
    
    def load_iot_data(
        self,
        network_id: int,
        devices_data: List[Dict[str, Any]],
        connections_data: List[Tuple[int, int]],
        data_sources_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Реализация прецедента "Загрузить данные об IoT-устройствах и -связях"
        
        Args:
            network_id: ID сети
            devices_data: Список словарей с данными устройств
            connections_data: Список кортежей (source_id, target_id)
            data_sources_data: Список словарей с данными источников
        
        Returns:
            Словарь с результатом операции
        """
        try:
            # 1. Проверить существование сети
            network_finder = IoTNetworkFinder(self.conn)
            network_gateway = network_finder.find_by_id(network_id)
            if not network_gateway:
                return {'success': False, 'error': f"Сеть с ID {network_id} не найдена"}
            
            # 2. Загрузить устройства
            device_gateways = []
            device_id_map = {}  # Соответствие оригинальных ID и реальных ID
            
            for device_data in devices_data:
                # Создать доменную модель устройства
                device = Device(
                    id=0,  # Временный ID
                    device_name=device_data['name'],
                    status=DeviceStatus(device_data.get('status', 'active')),
                    type=DeviceType(device_data['type']),
                    network_id=network_id,
                    connections=[]
                )
                
                # Создать и сохранить RDG
                device_gateway = DeviceGateway(self.conn)
                device_gateway.from_domain_model(device)
                device_id = device_gateway.insert()
                
                # Обновить ID в доменной модели
                device.id = device_id
                device_gateway.from_domain_model(device)
                
                device_gateways.append(device_gateway)
                device_id_map[device_data['original_id']] = device_id
            
            # 3. Загрузить связи между устройствами
            connection_count = 0
            connection_map = {}  # Для группировки связей по устройствам
            
            for source_orig_id, target_orig_id in connections_data:
                if source_orig_id in device_id_map and target_orig_id in device_id_map:
                    source_id = device_id_map[source_orig_id]
                    target_id = device_id_map[target_orig_id]
                    
                    if source_id not in connection_map:
                        connection_map[source_id] = []
                    
                    connection_map[source_id].append(target_id)
                    connection_count += 1
            
            # 4. Сохранить связи в БД
            for device_id, connections in connection_map.items():
                device_gateway = DeviceGateway(self.conn)
                if device_gateway.load(device_id):
                    device_gateway.save_connections(connections)
            
            # 5. Загрузить источники данных
            for ds_data in data_sources_data:
                data_source = DataSource(
                    id=0,
                    datasource_name=ds_data['name'],
                    last_update=datetime.fromisoformat(ds_data['last_update']),
                    type=DataSourceType(ds_data['type']),
                    network_id=network_id
                )
                
                data_source_gateway = DataSourceGateway(self.conn)
                data_source_gateway.from_domain_model(data_source)
                data_source_gateway.insert()
            
            self.conn.commit()
            
            return {
                'success': True,
                'message': f"Успешно загружено: {len(devices_data)} устройств, "
                          f"{connection_count} связей, {len(data_sources_data)} источников данных",
                'devices_loaded': len(devices_data),
                'connections_loaded': connection_count,
                'data_sources_loaded': len(data_sources_data)
            }
            
        except Exception as e:
            self.conn.rollback()
            return {'success': False, 'error': str(e)}
    
    # ===== ПРЕЦЕДЕНТ 2: АНАЛИЗ ТОПОЛОГИИ И СВЯЗЕЙ =====
    
    def analyze_topology_and_connections(self, network_id: int) -> Dict[str, Any]:
        """
        Реализация прецедента "Проанализировать топологию и связи"
        
        Args:
            network_id: ID сети для анализа
        
        Returns:
            Словарь с результатами анализа
        """
        try:
            # 1. Получить данные сети из БД через Finder
            network_finder = IoTNetworkFinder(self.conn)
            network_gateway = network_finder.find_by_id(network_id)
            
            if not network_gateway:
                return {'success': False, 'error': f"Сеть с ID {network_id} не найдена"}
            
            # 2. Получить доменную модель сети
            network = network_gateway.to_domain_model()
            if not network:
                return {'success': False, 'error': "Ошибка загрузки сети"}
            
            # 3. Получить устройства сети через Finder
            device_finder = DeviceFinder(self.conn)
            device_gateways = device_finder.find_by_network(network_id)
            
            if not device_gateways:
                return {'success': False, 'error': "В сети нет устройств для анализа"}
            
            # 4. Преобразовать RDG в доменные модели
            devices = []
            for device_gateway in device_gateways:
                device = device_gateway.to_domain_model()
                if device:
                    devices.append(device)
            
            # 5. Валидация сети (доменная логика)
            if not network.validate_connections(devices):
                return {'success': False, 'error': "Сеть содержит невалидные связи"}
            
            # 6. Выполнить анализ топологии (доменная логика)
            analysis_result = network.analyze_topology(devices)
            
            # 7. Сохранить результат анализа через RDG
            analysis_gateway = AnalysisGateway(self.conn)
            analysis_gateway.from_domain_model(analysis_result)
            analysis_id = analysis_gateway.insert()
            
            # 8. Сохранить детали анализа
            analysis_gateway.save_details(
                analysis_result.isolated_nodes,
                analysis_result.redundant_links
            )
            
            # 9. Получить источники данных для сводки
            data_source_finder = DataSourceFinder(self.conn)
            data_source_gateways = data_source_finder.find_by_network(network_id)
            data_sources = [ds.to_domain_model() for ds in data_source_gateways if ds.to_domain_model()]
            
            # 10. Подготовить детализированную информацию
            isolated_devices_info = self._get_device_details(analysis_result.isolated_nodes)
            redundant_links_info = self._get_link_details(analysis_result.redundant_links)
            
            # 11. Получить сводку по сети
            network_summary = network.get_network_summary(devices, data_sources)
            
            self.conn.commit()
            
            return {
                'success': True,
                'analysis_id': analysis_id,
                'network_name': network.network_name,
                'network_summary': network_summary,
                'analysis_date': analysis_result.date.strftime('%Y-%m-%d %H:%M:%S'),
                'centrality_score': round(analysis_result.centrality_score, 4),
                'isolated_nodes_count': len(analysis_result.isolated_nodes),
                'isolated_nodes': isolated_devices_info,
                'redundant_links_count': len(analysis_result.redundant_links),
                'redundant_links': redundant_links_info,
                'has_issues': analysis_result.has_issues(),
                'total_issues': analysis_result.get_issue_count(),
                'recommendations': analysis_result.get_recommendations()
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _get_device_details(self, device_ids: List[int]) -> List[Dict[str, Any]]:
        """Получить детальную информацию об устройствах"""
        device_finder = DeviceFinder(self.conn)
        device_details = []
        
        for device_id in device_ids:
            device_gateway = device_finder.find_by_id(device_id)
            if device_gateway:
                device = device_gateway.to_domain_model()
                if device:
                    device_details.append({
                        'id': device.id,
                        'name': device.device_name,
                        'type': device.type.value,
                        'status': device.status.value,
                        'is_active': device.is_active()
                    })
        
        return device_details
    
    def _get_link_details(self, links: List[Tuple[int, int]]) -> List[Dict[str, Any]]:
        """Получить детальную информацию о связях"""
        link_details = []
        
        for device1_id, device2_id in links:
            device1_gateway = DeviceGateway(self.conn)
            device2_gateway = DeviceGateway(self.conn)
            
            device1_name = "Неизвестно"
            device2_name = "Неизвестно"
            
            if device1_gateway.load(device1_id):
                device1 = device1_gateway.to_domain_model()
                if device1:
                    device1_name = device1.device_name
            
            if device2_gateway.load(device2_id):
                device2 = device2_gateway.to_domain_model()
                if device2:
                    device2_name = device2.device_name
            
            link_details.append({
                'device1_id': device1_id,
                'device2_id': device2_id,
                'device1_name': device1_name,
                'device2_name': device2_name
            })
        
        return link_details
    
    # ===== ДОПОЛНИТЕЛЬНЫЕ МЕТОДЫ ДЛЯ ВЕБ-ИНТЕРФЕЙСА =====
    
    def authenticate_user(self, login: str, password: str) -> Optional[User]:
        """Аутентификация пользователя"""
        user_finder = UserFinder(self.conn)
        user_gateway = user_finder.find_by_login(login)
        
        if user_gateway and user_gateway.password == password:
            return user_gateway.to_domain_model()
        return None
    
    def create_network(self, name: str, description: str = "", user_id: Optional[int] = None) -> Dict[str, Any]:
        """Создать новую IoT сеть"""
        try:
            # Создать доменную модель
            network = IoTNetwork(
                id=0,
                description=description,
                network_name=name,
                user_id=user_id
            )
            
            # Сохранить через RDG
            network_gateway = IoTNetworkGateway(self.conn)
            network_gateway.from_domain_model(network)
            network_id = network_gateway.insert()
            
            self.conn.commit()
            
            return {
                'success': True,
                'network_id': network_id,
                'network_name': name
            }
            
        except Exception as e:
            self.conn.rollback()
            return {'success': False, 'error': str(e)}
    
    def get_user_networks(self, user_id: int) -> List[Dict[str, Any]]:
        """Получить все сети пользователя"""
        network_finder = IoTNetworkFinder(self.conn)
        network_gateways = network_finder.find_by_user(user_id)
        
        networks = []
        for gateway in network_gateways:
            network = gateway.to_domain_model()
            if network:
                networks.append({
                    'id': network.id,
                    'name': network.network_name,
                    'description': network.description,
                    'created_at': network.created_at.isoformat() if network.created_at else None
                })
        
        return networks
    
    def get_network_details(self, network_id: int) -> Dict[str, Any]:
        """Получить детальную информацию о сети"""
        # Получить сеть
        network_finder = IoTNetworkFinder(self.conn)
        network_gateway = network_finder.find_by_id(network_id)
        
        if not network_gateway:
            return {}
        
        network = network_gateway.to_domain_model()
        if not network:
            return {}
        
        # Получить устройства
        device_finder = DeviceFinder(self.conn)
        device_gateways = device_finder.find_by_network(network_id)
        devices = [gateway.to_domain_model() for gateway in device_gateways if gateway.to_domain_model()]
        
        # Получить источники данных
        data_source_finder = DataSourceFinder(self.conn)
        data_source_gateways = data_source_finder.find_by_network(network_id)
        data_sources = [gateway.to_domain_model() for gateway in data_source_gateways if gateway.to_domain_model()]
        
        # Получить историю анализов
        analysis_finder = AnalysisFinder(self.conn)
        analysis_gateways = analysis_finder.find_by_network(network_id, limit=5)
        analyses = [gateway.to_domain_model() for gateway in analysis_gateways if gateway.to_domain_model()]
        
        # Статистика связей
        total_connections = sum(len(device.connections) for device in devices)
        
        return {
            'network': {
                'id': network.id,
                'name': network.network_name,
                'description': network.description,
                'created_at': network.created_at.isoformat() if network.created_at else None
            },
            'devices': [
                {
                    'id': device.id,
                    'name': device.device_name,
                    'type': device.type.value,
                    'status': device.status.value,
                    'connections_count': len(device.connections)
                }
                for device in devices
            ],
            'data_sources': [
                {
                    'id': ds.id,
                    'name': ds.datasource_name,
                    'type': ds.type.value,
                    'last_update': ds.last_update.isoformat()
                }
                for ds in data_sources
            ],
            'analyses': [
                {
                    'id': analysis.id,
                    'date': analysis.date.isoformat(),
                    'centrality_score': analysis.centrality_score,
                    'has_issues': analysis.has_issues()
                }
                for analysis in analyses
            ],
            'stats': {
                'total_devices': len(devices),
                'active_devices': len([d for d in devices if d.is_active()]),
                'total_data_sources': len(data_sources),
                'total_analyses': len(analyses),
                'total_connections': total_connections
            }
        }
    
    # Добавить этот метод в класс IoTNetworkApplicationService в файле application_service.py

    def get_all_networks(self, user_id: Optional[int] = None) -> List[Dict]:
        """Получить все сети (для совместимости со старым кодом)"""
        return self.get_user_networks(user_id) if user_id else []
    
    def get_data_sources_info(self, network_id: int) -> List[Dict[str, Any]]:
        """Получить информацию об источниках данных сети"""
        data_source_finder = DataSourceFinder(self.conn)
        data_source_gateways = data_source_finder.find_by_network(network_id)
        
        sources_info = []
        for gateway in data_source_gateways:
            source = gateway.to_domain_model()
            if source:
                sources_info.append({
                    'id': source.id,
                    'name': source.datasource_name,
                    'type': source.type.value,
                    'last_update': source.last_update.isoformat(),
                    'needs_update': source.needs_update()
                })
        
        return sources_info
    
    def close(self):
        """Закрыть соединение с БД"""
        self.conn.close()