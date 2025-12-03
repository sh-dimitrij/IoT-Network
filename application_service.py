from typing import List, Dict, Any, Optional, Tuple, Set
from datetime import datetime, timedelta
import sqlite3
from domain_models import (
    User, UserRole,
    Device, DeviceStatus, DeviceType,
    DataSource, DataSourceType,
    IoTNetwork, Analysis
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
                FOREIGN KEY (device_id) REFERENCES devices (id) ON DELETE CASCADE,
                UNIQUE(device_id, connected_device_id)
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
        cursor.execute("SELECT COUNT(*) FROM users WHERE login='admin'")
        if cursor.fetchone()[0] == 0:
            cursor.execute('''
                INSERT INTO users (name, login, password, role)
                VALUES (?, ?, ?, ?)
            ''', ('Администратор', 'admin', 'admin123', 'admin'))
            
            cursor.execute('''
                INSERT INTO users (name, login, password, role)
                VALUES (?, ?, ?, ?)
            ''', ('Аналитик', 'analyst', 'analyst123', 'analyst'))
        
        self.conn.commit()
    
    
    def add_device(self, network_id: int, device_data: Dict[str, Any]) -> Dict[str, Any]:
        """Добавить устройство вручную (с использованием Gateway)"""
        try:
            # Проверить существование сети через Finder
            network_finder = IoTNetworkFinder(self.conn)
            network_gateway = network_finder.find_by_id(network_id)
            
            if not network_gateway:
                return {'success': False, 'error': f'Сеть с ID {network_id} не найдена'}
            
            # Создать доменную модель устройства
            device = Device(
                id=None,
                device_name=device_data['name'],
                status=DeviceStatus(device_data.get('status', 'active')),
                type=DeviceType(device_data['type']),
                network_id=network_id,
                connections=device_data.get('connections', [])
            )
            
            # Сохранить через Gateway
            device_gateway = DeviceGateway(self.conn)
            device_gateway.from_domain_model(device)
            device_id = device_gateway.insert()
            
            # Сохранить связи устройства
            connections = device_data.get('connections', [])
            if connections:
                device_gateway.save_connections(connections)
            
            return {
                'success': True,
                'device_id': device_id,
                'message': f'Устройство "{device_data["name"]}" успешно добавлено'
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def remove_device(self, device_id: int) -> Dict[str, Any]:
        """Удалить устройство из сети (с использованием Gateway)"""
        try:

            device_finder = DeviceFinder(self.conn)
            device_gateway = device_finder.find_by_id(device_id)
            
            if not device_gateway:
                return {'success': False, 'error': f'Устройство с ID {device_id} не найдено'}

            device = device_gateway.to_domain_model()
            if not device:
                return {'success': False, 'error': 'Ошибка загрузки устройства'}
            
            device_name = device.device_name

            device_gateway.delete()
            
            return {
                'success': True,
                'message': f'Устройство "{device_name}" удалено'
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    
    def add_data_source(self, network_id: int, source_data: Dict[str, Any]) -> Dict[str, Any]:
        """Добавить источник данных (с использованием Gateway)"""
        try:

            network_finder = IoTNetworkFinder(self.conn)
            if not network_finder.find_by_id(network_id):
                return {'success': False, 'error': f'Сеть с ID {network_id} не найдена'}
            

            data_source = DataSource(
                id=None,
                datasource_name=source_data['name'],
                last_update=datetime.fromisoformat(source_data.get('last_update', datetime.now().isoformat())),
                type=DataSourceType(source_data['type']),
                network_id=network_id
            )
            

            source_gateway = DataSourceGateway(self.conn)
            source_gateway.from_domain_model(data_source)
            source_id = source_gateway.insert()
            
            return {
                'success': True,
                'source_id': source_id,
                'message': f'Источник данных "{source_data["name"]}" успешно добавлен'
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def remove_data_source(self, source_id: int) -> Dict[str, Any]:
        """Удалить источник данных (с использованием Gateway)"""
        try:

            source_finder = DataSourceFinder(self.conn)
            source_gateway = source_finder.find_by_id(source_id)
            
            if not source_gateway:
                return {'success': False, 'error': f'Источник данных с ID {source_id} не найден'}
            

            data_source = source_gateway.to_domain_model()
            if not data_source:
                return {'success': False, 'error': 'Ошибка загрузки источника данных'}
            
            source_name = data_source.datasource_name
            

            source_gateway.delete()
            
            return {
                'success': True,
                'message': f'Источник данных "{source_name}" удален'
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    
    def get_sample_datasets(self) -> Dict[str, Dict[str, Any]]:
        """Получить наборы примерных данных"""
        return {
            'smart_home': {
                'name': 'Умный дом',
                'description': 'Базовая сеть умного дома с датчиками и исполнительными устройствами',
                'devices': [
                    {'original_id': 101, 'name': 'Датчик температуры', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 102, 'name': 'Датчик влажности', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 103, 'name': 'Датчик движения', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 104, 'name': 'Умная лампа', 'type': 'actuator', 'status': 'active'},
                    {'original_id': 105, 'name': 'Кондиционер', 'type': 'actuator', 'status': 'active'},
                    {'original_id': 106, 'name': 'Шлюз Zigbee', 'type': 'gateway', 'status': 'active'},
                    {'original_id': 107, 'name': 'Розетка умная', 'type': 'actuator', 'status': 'inactive'},
                ],
                'connections': [
                    (101, 106), (102, 106), (103, 106), (106, 104), 
                    (106, 105), (101, 105), (104, 105)
                ],
                'data_sources': [
                    {'name': 'Home Assistant API', 'type': 'api', 'last_update': (datetime.now() - timedelta(hours=1)).isoformat()},
                    {'name': 'Zigbee Gateway', 'type': 'stream', 'last_update': datetime.now().isoformat()}
                ]
            },
            'industrial_iot': {
                'name': 'Промышленный IoT',
                'description': 'Сеть промышленных датчиков и контроллеров',
                'devices': [
                    {'original_id': 201, 'name': 'Датчик вибрации', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 202, 'name': 'Датчик давления', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 203, 'name': 'Датчик температуры', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 204, 'name': 'Контроллер PLC', 'type': 'controller', 'status': 'active'},
                    {'original_id': 205, 'name': 'Шлюз OPC UA', 'type': 'gateway', 'status': 'active'},
                    {'original_id': 206, 'name': 'Аварийный стоп', 'type': 'actuator', 'status': 'active'},
                    {'original_id': 207, 'name': 'Клапан регулирующий', 'type': 'actuator', 'status': 'active'},
                    {'original_id': 208, 'name': 'Резервный датчик', 'type': 'sensor', 'status': 'inactive'},
                ],
                'connections': [
                    (201, 205), (202, 205), (203, 205), (205, 204),
                    (204, 206), (204, 207), (201, 204), (202, 204),
                    (203, 204), (205, 206), (205, 207)
                ],
                'data_sources': [
                    {'name': 'OPC UA Server', 'type': 'stream', 'last_update': datetime.now().isoformat()},
                    {'name': 'SCADA Database', 'type': 'database', 'last_update': (datetime.now() - timedelta(hours=3)).isoformat()},
                    {'name': 'MES System API', 'type': 'api', 'last_update': (datetime.now() - timedelta(hours=2)).isoformat()}
                ]
            },
            'smart_city': {
                'name': 'Умный город',
                'description': 'Сеть устройств для управления городской инфраструктурой',
                'devices': [
                    {'original_id': 301, 'name': 'Умный фонарь', 'type': 'actuator', 'status': 'active'},
                    {'original_id': 302, 'name': 'Датчик освещенности', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 303, 'name': 'Датчик шума', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 304, 'name': 'Камера наблюдения', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 305, 'name': 'Контроллер района', 'type': 'controller', 'status': 'active'},
                    {'original_id': 306, 'name': 'Шлюз LoRaWAN', 'type': 'gateway', 'status': 'active'},
                    {'original_id': 307, 'name': 'Метеостанция', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 308, 'name': 'Табло информации', 'type': 'actuator', 'status': 'active'},
                ],
                'connections': [
                    (301, 306), (302, 306), (303, 306), (304, 306),
                    (307, 306), (306, 305), (305, 301), (305, 308),
                    (302, 305), (307, 305), (301, 302), (305, 306)
                ],
                'data_sources': [
                    {'name': 'LoRaWAN Network Server', 'type': 'stream', 'last_update': datetime.now().isoformat()},
                    {'name': 'City Management API', 'type': 'api', 'last_update': (datetime.now() - timedelta(hours=1)).isoformat()},
                    {'name': 'Weather Service', 'type': 'api', 'last_update': (datetime.now() - timedelta(minutes=30)).isoformat()}
                ]
            },
            'simple_test': {
                'name': 'Простой тест',
                'description': 'Минимальная сеть для тестирования',
                'devices': [
                    {'original_id': 401, 'name': 'Датчик 1', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 402, 'name': 'Датчик 2', 'type': 'sensor', 'status': 'active'},
                    {'original_id': 403, 'name': 'Контроллер', 'type': 'controller', 'status': 'active'},
                ],
                'connections': [
                    (401, 403), (402, 403)
                ],
                'data_sources': [
                    {'name': 'Test API', 'type': 'api', 'last_update': datetime.now().isoformat()}
                ]
            }
        }
    
    
    def load_iot_data(
        self,
        network_id: int,
        dataset_name: str = None
    ) -> Dict[str, Any]:
        """
        Реализация прецедента "Загрузить данные об IoT-устройствах и -связях"
        С ИСПОЛЬЗОВАНИЕМ Gateway/Finders/Domain Models
        """
        try:
            network_finder = IoTNetworkFinder(self.conn)
            network_gateway = network_finder.find_by_id(network_id)
            
            if not network_gateway:
                return {'success': False, 'error': f"Сеть с ID {network_id} не найдена"}

            if not dataset_name:
                return {'success': False, 'error': "Не указан набор данных"}
            
            datasets = self.get_sample_datasets()
            if dataset_name not in datasets:
                return {'success': False, 'error': f"Набор данных '{dataset_name}' не найден"}
            
            dataset = datasets[dataset_name]
            devices_data = dataset['devices']
            connections_data = dataset['connections']
            data_sources_data = dataset['data_sources']
            

            self.conn.execute('BEGIN TRANSACTION')

            device_finder = DeviceFinder(self.conn)
            existing_device_gateways = device_finder.find_by_network(network_id)
            
            for device_gateway in existing_device_gateways:
                device_gateway.delete()
            
            data_source_finder = DataSourceFinder(self.conn)
            existing_source_gateways = data_source_finder.find_by_network(network_id)
            
            for source_gateway in existing_source_gateways:
                source_gateway.delete()

            device_id_map = {}
            
            for device_data in devices_data:
                device = Device(
                    id=None,
                    device_name=device_data['name'],
                    status=DeviceStatus(device_data.get('status', 'active')),
                    type=DeviceType(device_data['type']),
                    network_id=network_id,
                    connections=[]
                )
                
                device_gateway = DeviceGateway(self.conn)
                device_gateway.from_domain_model(device)
                device_id = device_gateway.insert()
                device_id_map[device_data['original_id']] = device_id

            connection_count = 0
            seen_connections: Set[Tuple[int, int]] = set()
            
            for source_orig_id, target_orig_id in connections_data:
                if source_orig_id in device_id_map and target_orig_id in device_id_map:
                    source_id = device_id_map[source_orig_id]
                    target_id = device_id_map[target_orig_id]
                    
                    connection_key = tuple(sorted((source_id, target_id)))
                    if connection_key in seen_connections:
                        continue
                    
                    seen_connections.add(connection_key)
                    
                    source_gateway = device_finder.find_by_id(source_id)
                    if not source_gateway:
                        continue
                    
                    source_device = source_gateway.to_domain_model()
                    if not source_device:
                        continue
                    
                    source_device.add_connection(target_id)
                    source_gateway.from_domain_model(source_device)
                    source_gateway.save_connections(source_device.connections)
                    
                    connection_count += 1

            for ds_data in data_sources_data:
                data_source = DataSource(
                    id=None,
                    datasource_name=ds_data['name'],
                    last_update=datetime.fromisoformat(ds_data['last_update']),
                    type=DataSourceType(ds_data['type']),
                    network_id=network_id
                )
                
                source_gateway = DataSourceGateway(self.conn)
                source_gateway.from_domain_model(data_source)
                source_gateway.insert()
            
            self.conn.commit()

            final_device_count = len(device_finder.find_by_network(network_id))
            final_source_count = len(data_source_finder.find_by_network(network_id))
            
            return {
                'success': True,
                'message': f"Успешно загружено из '{dataset['name']}': {final_device_count} устройств, "
                          f"{connection_count} связей, {final_source_count} источников данных",
                'devices_loaded': final_device_count,
                'connections_loaded': connection_count,
                'data_sources_loaded': final_source_count,
                'dataset_name': dataset['name']
            }
            
        except Exception as e:
            self.conn.rollback()
            return {'success': False, 'error': str(e)}
    
    def analyze_topology_and_connections(self, network_id: int) -> Dict[str, Any]:
        """
        Реализация прецедента "Проанализировать топологию и связи"
        С ИСПОЛЬЗОВАНИЕМ Gateway/Finders/Domain Models
        """
        try:
            # 1. Получить данные сети через Finder
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
            
            # 4. Преобразовать Gateway в доменные модели
            devices = []
            for device_gateway in device_gateways:
                device = device_gateway.to_domain_model()
                if device:
                    devices.append(device)
            
            # 5. Валидация сети
            if not network.validate_connections(devices):
                return {'success': False, 'error': "Сеть содержит невалидные связи"}
            
            # 6. Выполнить анализ топологии
            analysis_result = network.analyze_topology(devices)
            
            # 7. Сохранить результат анализа через Gateway
            analysis_gateway = AnalysisGateway(self.conn)
            analysis_gateway.centrality_score = analysis_result.centrality_score
            analysis_gateway.date = analysis_result.date.isoformat()
            analysis_gateway.network_id = network_id
            
            analysis_id = analysis_gateway.insert()
            
            # 8. Сохранить детали анализа через Gateway
            analysis_gateway.save_details(
                analysis_result.isolated_nodes,
                analysis_result.redundant_links
            )
            
            # 9. Подготовить детализированную информацию
            isolated_devices_info = []
            for node_id in analysis_result.isolated_nodes:
                device_gateway = device_finder.find_by_id(node_id)
                if device_gateway:
                    device = device_gateway.to_domain_model()
                    if device:
                        isolated_devices_info.append({
                            'id': device.id,
                            'name': device.device_name,
                            'type': device.type.value,
                            'status': device.status.value,
                            'is_active': device.is_active()
                        })
            
            redundant_links_info = []
            for link in analysis_result.redundant_links:
                device1_gateway = device_finder.find_by_id(link[0])
                device2_gateway = device_finder.find_by_id(link[1])
                
                device1_name = "Неизвестно"
                device2_name = "Неизвестно"
                
                if device1_gateway:
                    device1 = device1_gateway.to_domain_model()
                    if device1:
                        device1_name = device1.device_name
                
                if device2_gateway:
                    device2 = device2_gateway.to_domain_model()
                    if device2:
                        device2_name = device2.device_name
                
                redundant_links_info.append({
                    'device1_id': link[0],
                    'device2_id': link[1],
                    'device1_name': device1_name,
                    'device2_name': device2_name
                })
            
            # 10. Получить источники данных через Finder
            data_source_finder = DataSourceFinder(self.conn)
            data_source_gateways = data_source_finder.find_by_network(network_id)
            data_sources = [ds.to_domain_model() for ds in data_source_gateways if ds.to_domain_model()]
            
            # 11. Получить сводку по сети
            network_summary = network.get_network_summary(devices, data_sources)
            
            # 12. Получить историю анализов через Finder
            analysis_finder = AnalysisFinder(self.conn)
            recent_analyses = analysis_finder.find_by_network(network_id, limit=5)
            
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
                'recommendations': analysis_result.get_recommendations(),
                'history_count': len(recent_analyses)
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    
    def authenticate_user(self, login: str, password: str) -> Optional[User]:
        """Аутентификация пользователя (с использованием Gateway/Finder)"""
        user_finder = UserFinder(self.conn)
        user_gateway = user_finder.find_by_login(login)
        
        if user_gateway and user_gateway.password == password:
            return user_gateway.to_domain_model()
        return None
    
    def create_network(self, name: str, description: str = "", user_id: Optional[int] = None) -> Dict[str, Any]:
        """Создать новую IoT сеть (с использованием Gateway)"""
        try:

            network = IoTNetwork(
                id=None,
                description=description,
                network_name=name,
                user_id=user_id
            )

            network_gateway = IoTNetworkGateway(self.conn)
            network_gateway.from_domain_model(network)
            network_id = network_gateway.insert()
            
            return {
                'success': True,
                'network_id': network_id,
                'network_name': name
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def get_user_networks(self, user_id: int) -> List[Dict[str, Any]]:
        """Получить все сети пользователя (с использованием Finder/Gateway)"""
        try:
            network_finder = IoTNetworkFinder(self.conn)
            network_gateways = network_finder.find_by_user(user_id)
            
            networks = []
            for network_gateway in network_gateways:
                network = network_gateway.to_domain_model()
                if network:
                    networks.append({
                        'id': network.id,
                        'name': network.network_name,
                        'description': network.description,
                        'created_at': network.created_at.isoformat() if network.created_at else None
                    })
            
            return networks
            
        except Exception as e:
            return []
    
    def get_network_details(self, network_id: int) -> Dict[str, Any]:
        """Получить детальную информацию о сети (с использованием Finders/Gateways)"""
        try:

            network_finder = IoTNetworkFinder(self.conn)
            network_gateway = network_finder.find_by_id(network_id)
            
            if not network_gateway:

                return {
                    'network': {},
                    'devices': [],
                    'data_sources': [],
                    'analyses': [],
                    'stats': {
                        'total_devices': 0,
                        'active_devices': 0,
                        'total_data_sources': 0,
                        'total_analyses': 0,
                        'total_connections': 0
                    }
                }
            
            network = network_gateway.to_domain_model()
            if not network:
                return {
                    'network': {},
                    'devices': [],
                    'data_sources': [],
                    'analyses': [],
                    'stats': {
                        'total_devices': 0,
                        'active_devices': 0,
                        'total_data_sources': 0,
                        'total_analyses': 0,
                        'total_connections': 0
                    }
                }
            

            device_finder = DeviceFinder(self.conn)
            device_gateways = device_finder.find_by_network(network_id)
            
            devices = []
            for device_gateway in device_gateways:
                device = device_gateway.to_domain_model()
                if device:

                    connections_count = len(device.connections)
                    devices.append({
                        'id': device.id,
                        'name': device.device_name,
                        'type': device.type.value,
                        'status': device.status.value,
                        'connections_count': connections_count
                    })
            

            data_source_finder = DataSourceFinder(self.conn)
            data_source_gateways = data_source_finder.find_by_network(network_id)
            
            data_sources = []
            for source_gateway in data_source_gateways:
                source = source_gateway.to_domain_model()
                if source:
                    data_sources.append({
                        'id': source.id,
                        'name': source.datasource_name,
                        'type': source.type.value,
                        'last_update': source.last_update.isoformat()
                    })
            

            analysis_finder = AnalysisFinder(self.conn)
            analysis_gateways = analysis_finder.find_by_network(network_id, limit=5)
            
            analyses = []
            for analysis_gateway in analysis_gateways:
                analysis = analysis_gateway.to_domain_model()
                if analysis:
                    analyses.append({
                        'id': analysis.id,
                        'date': analysis.date.isoformat(),
                        'centrality_score': analysis.centrality_score,
                        'has_issues': analysis.has_issues()
                    })
            
            active_devices = len([d for d in devices if d['status'] == 'active'])
            total_connections = sum(d['connections_count'] for d in devices)

            network_info = {
                'id': network.id,
                'name': network.network_name,
                'description': network.description or '',
                'created_at': network.created_at.isoformat() if network.created_at else ''
            }
            
            return {
                'network': network_info,
                'devices': devices,
                'data_sources': data_sources,
                'analyses': analyses,
                'stats': {
                    'total_devices': len(devices),
                    'active_devices': active_devices,
                    'total_data_sources': len(data_sources),
                    'total_analyses': len(analyses),
                    'total_connections': total_connections
                }
            }
            
        except Exception as e:

            return {
                'network': {},
                'devices': [],
                'data_sources': [],
                'analyses': [],
                'stats': {
                    'total_devices': 0,
                    'active_devices': 0,
                    'total_data_sources': 0,
                    'total_analyses': 0,
                    'total_connections': 0
                }
            }
    def delete_network(self, network_id: int) -> Dict[str, Any]:
        """Удалить IoT сеть (с использованием Gateway)"""
        try:

            network_finder = IoTNetworkFinder(self.conn)
            network_gateway = network_finder.find_by_id(network_id)
            
            if not network_gateway:
                return {'success': False, 'error': f'Сеть с ID {network_id} не найдена'}

            network = network_gateway.to_domain_model()
            if not network:
                return {'success': False, 'error': 'Ошибка загрузки сети'}
            
            network_name = network.network_name

            network_gateway.delete()
            
            return {
                'success': True,
                'message': f'Сеть "{network_name}" и все связанные данные успешно удалены'
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def close(self):
        """Закрыть соединение с БД"""
        self.conn.close()